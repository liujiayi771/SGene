package edu.hust.elwg

import java.io.File
import java.net.URISyntaxException

import edu.hust.elwg.tools._
import edu.hust.elwg.utils.{Logger, NGSSparkConf, NGSSparkFileUtils}
import htsjdk.samtools._
import org.apache.spark.SparkConf

import scala.io.Source

class VariantCalling(settings: Array[(String, String)], regionId: Int) {
  val conf = new SparkConf()
  conf.setAll(settings)
  val chrId: Int = if (regionId <= NGSSparkConf.getChromosomeNum(conf)) regionId else regionId - NGSSparkConf.getChromosomeNum(conf)
  val localTmp: String = NGSSparkConf.getLocalTmp(conf)
  val readGroupIdSet: Array[String] = NGSSparkConf.getReadGroupId(conf)

  val randomString: String = CommandGenerator.randomString(8)
  val tmpFileBase: String = localTmp + randomString + "-" + regionId

  val bin: String = NGSSparkConf.getBin(conf)
  val index: String = NGSSparkConf.getIndex(conf)

  val useSplitTargetBed: Boolean = NGSSparkConf.getUseLocalCProgram(conf)

  val TARGET_BED_DIR: String = localTmp + "targetBed/"
  val SORT_DIR: String = localTmp + "sort_bam/"
  val UN_SORT_DIR: String = localTmp + "un_sort_bam/"
  val MARK_DUPLICATES_DIR: String = localTmp + "mark_duplicates_bam/"
  val MARK_DUPLICATES_METRICS_DIR: String = localTmp + "mark_duplicates_metrics/"
  val INDEL_REALIGNMENT_DIR: String = localTmp + "indel_realignment_bam/"
  val BASE_RECALIBRATOR_TABLE: String = localTmp + "base_recalibrator_table/"
  val PRINT_READS_DIR: String = localTmp + "print_reads_bam/"
  val MUTECT2_DIR: String = localTmp + "vcf/"

  val dict: SAMSequenceDictionary = NGSSparkConf.getSequenceDictionary(conf)
  val header: SAMFileHeader = new SAMFileHeader()
  header.setSequenceDictionary(dict)

  val splitBed: Boolean = false
  val keep: Boolean = false
  val upload: Boolean = true
  val keepDups: Boolean = true

  if (readGroupIdSet.isEmpty || readGroupIdSet.length > 2) throw new Exception("Please specify one or two read group information")

  def variantCallFirstHalf(unsortedSamRecords: Iterable[MySAMRecord]): (Int, String, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)
    Logger.INFOTIME("size: " + unsortedSamRecords.size)

    implicit val samRecordOrdering: Ordering[MySAMRecord] = new Ordering[MySAMRecord] {
      override def compare(x: MySAMRecord, y: MySAMRecord): Int = {
        if (x.referenceIndex != y.referenceIndex) x.referenceIndex - y.referenceIndex
        else {
          if (x.startPos != y.startPos) {
            x.startPos - y.startPos
          } else {
            if (x.originalStr > y.originalStr) 1 else -1
          }
        }
      }
    }
    val samRecordsListOne = unsortedSamRecords.filter(_.RGID == readGroupIdSet(0)).toArray
    val samRecordsListTwo = unsortedSamRecords.filter(_.RGID == readGroupIdSet(1)).toArray

    writeUnSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    writeUnSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    scala.util.Sorting.quickSort(samRecordsListOne)
    scala.util.Sorting.quickSort(samRecordsListTwo)

    // Write the sorted sam records to disk in bam format
    val sortedBamFileOne = writeSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    val sortedBamFileTwo = writeSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), sortedBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallFirstHalfSamtoolsSort(unsortedSamRecords: Iterable[MySAMRecord]): (Int, String, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)
    Logger.INFOTIME("size: " + unsortedSamRecords.size)
    val tools = new PreprocessTools(bin, conf)

    val samRecordsListOne = unsortedSamRecords.filter(_.RGID == readGroupIdSet(0)).toArray
    val samRecordsListTwo = unsortedSamRecords.filter(_.RGID == readGroupIdSet(1)).toArray

    val unsortedBamOne = writeUnSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    val unsortedBamTwo = writeUnSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    val sortedBamFileOne = tmpFileBase + "-" + NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)).RGID + "-sorted.bam"
    val sortedBamFileTwo = tmpFileBase + "-" + NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)).RGID + "-sorted.bam"

    tools.runSortBamSamtools(unsortedBamOne, sortedBamFileOne, 6)
    tools.runSortBamSamtools(unsortedBamTwo, sortedBamFileTwo, 6)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), sortedBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallSecondHalf(inputBamFileOne: String, inputBamFileTwo: String, inputTableOne: String, inputTableTwo: String): Unit = {
    Logger.INFOTIME("Processing chromosome region [second]: " + regionId)

    val tools = new PreprocessTools(bin, conf)

    tools.runBuildBamIndexPicard(inputBamFileOne)
    tools.runBuildBamIndexPicard(inputBamFileTwo)

    // Run printReads
    val printReadsFileOne = printReads(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), inputBamFileOne, inputTableOne)
    val printReadsFileTwo = printReads(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), inputBamFileTwo, inputTableTwo)

    // Run mutect2
    val vcfOutputFile = mutect2(printReadsFileOne, printReadsFileTwo)

    if (new File(vcfOutputFile).exists()) {
      writeVCFOutputFile(vcfOutputFile)
    }
  }

  def singleInputVariantCallFirstHalf(unsortedSamRecords: Iterable[MySAMRecord]): (Int, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)
    Logger.INFOTIME("size: " + unsortedSamRecords.size)

    implicit val samRecordOrdering: Ordering[MySAMRecord] = new Ordering[MySAMRecord] {
      override def compare(x: MySAMRecord, y: MySAMRecord): Int = {
        if (x.referenceIndex != y.referenceIndex) x.referenceIndex - y.referenceIndex
        else {
          if (x.startPos != y.startPos) {
            x.startPos - y.startPos
          } else {
            if (x.originalStr > y.originalStr) 1 else -1
          }
        }
      }
    }
    val samRecordsList = unsortedSamRecords.toArray

    writeUnSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsList)

    scala.util.Sorting.quickSort(samRecordsList)

    // Write the sorted sam records to disk in bam format
    val sortedBamFile = writeSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsList)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFile)

    // Run indelRealignment
    val indelRealignmentOut = indelRealignment(markDuplicatesBamFileOne)

    // Run baseRecalibrator
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentOut)

    (regionId, indelRealignmentOut)
  }

  def singleInputVariantCallSecondHalf(inputBamFile: String, inputTable: String): Unit = {
    Logger.INFOTIME("Processing chromosome region [second]: " + regionId)

    val tools = new PreprocessTools(bin, conf)

    tools.runBuildBamIndexPicard(inputBamFile)

    // Run printReads
    val printReadsFile = printReads(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), inputBamFile, inputTable)

    // Run HaplotypeCaller
    val vcfOutputFile = haplotypeCaller(printReadsFile)

    if (new File(vcfOutputFile).exists()) {
      writeVCFOutputFile(vcfOutputFile)
    }
  }

  def wholeGenomeVariantCall(unsortedSamRecords: Iterable[MySAMRecord]): Unit = {
    Logger.INFOTIME("Processing chromosome region [whole genome]: " + regionId)

    // Sorting
    implicit val samRecordOrdering: Ordering[MySAMRecord] = new Ordering[MySAMRecord] {
      override def compare(x: MySAMRecord, y: MySAMRecord): Int = {
        if (x.referenceIndex != y.referenceIndex) x.referenceIndex - y.referenceIndex
        else {
          if (x.startPos != y.startPos) {
            x.startPos - y.startPos
          } else {
            if (x.originalStr > y.originalStr) 1 else -1
          }
        }
      }
    }
    val samRecordsListOne = unsortedSamRecords.filter(_.RGID == readGroupIdSet(0)).toArray
    val samRecordsListTwo = unsortedSamRecords.filter(_.RGID == readGroupIdSet(1)).toArray

    scala.util.Sorting.quickSort(samRecordsListOne)
    scala.util.Sorting.quickSort(samRecordsListTwo)

    // Write the sorted sam records to disk in bam format
    val sortedBamFileOne = writeSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    val sortedBamFileTwo = writeSortedBamFile(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), sortedBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseQualityScoreRecalibration
    val baseQualityScoreRecalibrationBamFileOne = baseQualityScoreRecalibration(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    val baseQualityScoreRecalibrationBamFileTwo = baseQualityScoreRecalibration(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    // Run mutect2
    val vcfOutputFile = mutect2(baseQualityScoreRecalibrationBamFileOne, baseQualityScoreRecalibrationBamFileTwo)

    writeVCFOutputFile(vcfOutputFile)
  }

  def variantCallFromMarkDuplicatesFirstHalf(inputBamFileOne: String, inputBamFileTwo: String): (Int, String, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Build bam index
    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), localInputBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), localInputBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallFromIndelRealignmentFirstHalf(inputBamFileOne: String, inputBamFileTwo: String): (Int, String, String) = {
    Logger.INFOTIME("[1] " + inputBamFileOne + "   [2] " + inputBamFileTwo)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Build bam index
    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(localInputBamFileOne, localInputBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallFromBQSRFirstHalf(inputBamFileOne: String, inputBamFileTwo: String): (Int, String, String) = {
    Logger.INFOTIME("[1] " + inputBamFileOne + "   [2] " + inputBamFileTwo)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Build bam index
    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Run baseRecalibrator
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(0)), localInputBamFileOne)
    baseRecalibrator(NGSSparkConf.getReadGroup(conf, readGroupIdSet(1)), localInputBamFileTwo)

    (regionId, inputBamFileOne, inputBamFileTwo)
  }

  def variantCallFromMutect2(inputBamFileOne: String, inputBamFileTwo: String): Unit = {
    Logger.INFOTIME("[1] " + inputBamFileOne + "   [2] " + inputBamFileTwo)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    NGSSparkFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Run mutect2
    val vcfOutputFile = mutect2(localInputBamFileOne, localInputBamFileTwo)

    if (new File(vcfOutputFile).exists()) {
      writeVCFOutputFile(vcfOutputFile)
    }
  }

  def writeSortedBamFile(rg: ReadGroup, sortedSamRecords: Array[MySAMRecord]): String = {
    NGSSparkFileUtils.mkLocalDir(SORT_DIR, delete = false)

    val factory: SAMFileWriterFactory = new SAMFileWriterFactory()
    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    outHeader.addReadGroup(rg.getSAMReadGroupRecord())

    val sortOutFile = tmpFileBase + "-" + rg.RGID + "-sorted.bam"
    val nfsSortOutFile = SORT_DIR + sortOutFile.split("/").last

    val writer: SAMFileWriter = factory.makeBAMWriter(outHeader, true, new File(nfsSortOutFile))
    val samRecordFactory: SAMRecordFactory = new DefaultSAMRecordFactory()
    val validationStringency: ValidationStringency = ValidationStringency.LENIENT
    val parser: SAMLineParser = new SAMLineParser(samRecordFactory, validationStringency, outHeader, null, null)

    for (mySamRecord <- sortedSamRecords) {
      val sam = parser.parseLine(mySamRecord.originalStr)
      writer.addAlignment(sam)
    }

    writer.close()

    nfsSortOutFile
  }

  def readStream(rg: Int, samRecord: Iterator[String]): List[(Int, MySAMRecord)] = {
    var samRecordList: List[(Int, MySAMRecord)] = Nil

    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    outHeader.addReadGroup(NGSSparkConf.getReadGroup(conf, readGroupIdSet(rg)).getSAMReadGroupRecord())
    val samRecordFactory: SAMRecordFactory = new DefaultSAMRecordFactory()
    val validationStringency: ValidationStringency = ValidationStringency.LENIENT
    val parser: SAMLineParser = new SAMLineParser(samRecordFactory, validationStringency, outHeader, null, null)

    for (itr <- samRecord) {
      val sam = parser.parseLine(itr)
      val referenceIndex: Int = sam.getReferenceIndex.toInt
      val read1Ref = sam.getReferenceIndex
      val read2Ref = sam.getMateReferenceIndex
      if (!sam.getReadUnmappedFlag &&
        (read1Ref >= 0 || read2Ref >= 0)) {
        val chr = if (referenceIndex >= 0 && referenceIndex < NGSSparkConf.getChromosomeNum(conf)) referenceIndex + 1 else 99
        samRecordList = (chr, new MySAMRecord(sam, itr, mateReference = true)) :: samRecordList
      }
    }
    samRecordList
  }

  def writeUnSortedBamFile(rg: ReadGroup, sortedSamRecords: Array[MySAMRecord]): String = {
    NGSSparkFileUtils.mkLocalDir(UN_SORT_DIR, delete = false)

    val factory: SAMFileWriterFactory = new SAMFileWriterFactory()
    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.unsorted)
    outHeader.addReadGroup(rg.getSAMReadGroupRecord())

    val unSortOutFile = tmpFileBase + "-" + rg.RGID + "-unsorted.bam"
    val nfsUnSortOutFile = UN_SORT_DIR + unSortOutFile.split("/").last

    val writer: SAMFileWriter = factory.makeBAMWriter(outHeader, true, new File(nfsUnSortOutFile))
    val samRecordFactory: SAMRecordFactory = new DefaultSAMRecordFactory()
    val validationStringency: ValidationStringency = ValidationStringency.LENIENT
    val parser: SAMLineParser = new SAMLineParser(samRecordFactory, validationStringency, outHeader, null, null)

    for (mySamRecord <- sortedSamRecords) {
      val sam = parser.parseLine(mySamRecord.originalStr)
      writer.addAlignment(sam)
    }

    writer.close()

    nfsUnSortOutFile
  }

  def markDuplicates(rg: ReadGroup, inputBamFile: String): String = {
    NGSSparkFileUtils.mkLocalDir(MARK_DUPLICATES_DIR, delete = false)
    NGSSparkFileUtils.mkLocalDir(MARK_DUPLICATES_METRICS_DIR, delete = false)

    val tools = new PreprocessTools(bin, conf)

    val markDuplicatesOutFile = tmpFileBase + "-" + rg.RGID + "-markDuplicates.bam"
    val nfsMarkDuplicatesOutFile = MARK_DUPLICATES_DIR + markDuplicatesOutFile.split("/").last

    val markDuplicatesMetricsFile = tmpFileBase + "-" + rg.RGID + "-metrics.txt"
    val nfsMarkDuplicatesMetricsFile = MARK_DUPLICATES_METRICS_DIR + markDuplicatesMetricsFile.split("/").last

    tools.runMarkDuplicates(inputBamFile, nfsMarkDuplicatesOutFile, nfsMarkDuplicatesMetricsFile, keepDups = false)

    // Generate the bai file of the bam file
    tools.runBuildBamIndexPicard(nfsMarkDuplicatesOutFile)

    nfsMarkDuplicatesOutFile
  }

  def indelRealignment(inputBamFileOne: String, inputBamFileTwo: String): (String, String, String, String) = {
    NGSSparkFileUtils.mkLocalDir(INDEL_REALIGNMENT_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val targetsFile = tmpFileBase + ".intervals"
    val nWayOut = "_realign.bam"

    val outBamFileOne = inputBamFileOne.split("/").last.split('.').head + nWayOut
    val nfsOutBamFileOne = INDEL_REALIGNMENT_DIR + outBamFileOne

    val outBamFileTwo = inputBamFileTwo.split("/").last.split('.').head + nWayOut
    val nfsOutBamFileTwo = INDEL_REALIGNMENT_DIR + outBamFileTwo

    val bed =
      if (splitBed) {
        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runRealignerTargetCreator(inputBamFileOne, inputBamFileTwo, targetsFile, index, bed)

    gatk.runIndelRealigner(inputBamFileOne, inputBamFileTwo, targetsFile, nWayOut, index)

    NGSSparkFileUtils.copyFileInLocal(outBamFileOne, nfsOutBamFileOne, delete = true)
    NGSSparkFileUtils.copyFileInLocal(outBamFileTwo, nfsOutBamFileTwo, delete = true)

    (outBamFileOne, outBamFileTwo, nfsOutBamFileOne, nfsOutBamFileTwo)
  }

  def indelRealignment(inputBamFile: String): String = {
    NGSSparkFileUtils.mkLocalDir(INDEL_REALIGNMENT_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val targetsFile = tmpFileBase + ".intervals"
    val nWayOut = "_realign.bam"

    val outBamFile = inputBamFile.split("/").last.split('.').head + nWayOut
    val nfsOutBamFile = INDEL_REALIGNMENT_DIR + outBamFile

    val bed =
      if (splitBed) {
        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runRealignerTargetCreator(inputBamFile, targetsFile, index, bed)

    gatk.runIndelRealigner(inputBamFile, targetsFile, nfsOutBamFile, index)

    nfsOutBamFile
  }

  def baseQualityScoreRecalibration(rg: ReadGroup, inputBamFile: String): String = {
    NGSSparkFileUtils.mkHdfsDir(PRINT_READS_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val tableFile = tmpFileBase + "-" + rg.RGID + ".table"
    val outBamFile = tmpFileBase + "-" + rg.RGID + "-printreads.bam"
    val hdfsOutBamFile = PRINT_READS_DIR + outBamFile.split("/").last

    val bed =
      if (splitBed) {
        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runBaseRecalibrator(inputBamFile, tableFile, index, bed)
    gatk.runPrintReads(inputBamFile, outBamFile, index, tableFile)

    NGSSparkFileUtils.uploadFileToHdfs(outBamFile, hdfsOutBamFile, upload)

    NGSSparkFileUtils.deleteLocalFile(inputBamFile, keep)

    outBamFile
  }

  def baseRecalibrator(rg: ReadGroup, inputBamFile: String): Unit = {
    NGSSparkFileUtils.mkLocalDir(BASE_RECALIBRATOR_TABLE, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val tableFile = tmpFileBase + "-" + rg.RGID + ".table"
    val nfsTableFile = BASE_RECALIBRATOR_TABLE + tableFile.split("/").last

    val bed =
      if (splitBed) {
        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runBaseRecalibrator(inputBamFile, nfsTableFile, index, bed)

    NGSSparkFileUtils.deleteLocalFile(inputBamFile, keep)
    NGSSparkFileUtils.deleteLocalFile(inputBamFile.split('.').head + ".bai", keep)
  }

  def printReads(rg: ReadGroup, inputBamFile: String, table: String): String = {
    NGSSparkFileUtils.mkLocalDir(PRINT_READS_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val outBamFile = tmpFileBase + "-" + rg.RGID + "-printreads.bam"
    val nfsOutBamFile = PRINT_READS_DIR + outBamFile.split("/").last

    gatk.runPrintReads(inputBamFile, nfsOutBamFile, index, table)

    nfsOutBamFile
  }

  def mutect2(inputBamFileOne: String, inputBamFileTwo: String): String = {
    val gatk = new GATKTools(index, bin, conf)

    val vcfOutFile = tmpFileBase + ".vcf"

    val bed =
      if (useSplitTargetBed) {
        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runMuTect2(inputBamFileOne, inputBamFileTwo, vcfOutFile, index, bed)

    vcfOutFile
  }

  def haplotypeCaller(inputBamFile: String): String = {
    val gatk = new GATKTools(index, bin, conf)

    val vcfOutFile = tmpFileBase + ".vcf"

    val bed =
      if (useSplitTargetBed) {
        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runHaplotypeCaller(inputBamFile, vcfOutFile, index, bed)

    vcfOutFile
  }

  def writeVCFOutputFile(vcfOutputFile: String): Unit = {
    NGSSparkFileUtils.mkLocalDir(MUTECT2_DIR, delete = false)

    if (vcfOutputFile != "" && checkVcfIsNotEmpty(vcfOutputFile)) {
      try {
        val nfsVcfOutputFile = MUTECT2_DIR + vcfOutputFile.split("/").last
        NGSSparkFileUtils.copyFileInLocal(vcfOutputFile, nfsVcfOutputFile, delete = true)
        NGSSparkFileUtils.copyFileInLocal(vcfOutputFile + ".idx", nfsVcfOutputFile + ".idx", delete = true)
      } catch {
        case e: URISyntaxException =>
          Logger.EXCEPTION(e)
          throw new InterruptedException
      }
    } else if (vcfOutputFile != "") Logger.DEBUG("empty vcf file, not uploaded to vcf to avoid error when merging.")
  }

  def checkVcfIsNotEmpty(vcfFile: String): Boolean = Source.fromFile(vcfFile).getLines.exists(!_.startsWith("#"))
}
