import java.io.File
import java.net.URISyntaxException

import tools._
import utils.{Logger, SGeneConf, SGeneFileUtils}
import htsjdk.samtools._
import org.apache.spark.SparkConf

import scala.io.Source

class VariantCalling(settings: Array[(String, String)], regionId: Int) {
  val conf = new SparkConf()
  conf.setAll(settings)
  val chrId: Int = if (regionId <= ProgramVariable.CHR_NUM) regionId else regionId - ProgramVariable.CHR_NUM
  val localTmp: String = SGeneConf.getLocalTmp(conf)
  val hdfsTmp: String = SGeneConf.getHdfsTmp(conf)
  val readGroupIdSet: Array[String] = SGeneConf.getReadGroupId(conf)

  val randomString: String = CommandGenerator.randomString(8)
  val tmpFileBase: String = localTmp + randomString + "-" + regionId

  val bin: String = SGeneConf.getBin(conf)
  val index: String = SGeneConf.getIndex(conf)

  val useSplitTargetBed: Boolean = SGeneConf.getUseLocalCProgram(conf)

  val TARGET_BED_DIR: String = hdfsTmp + "targetBed/"
  val SORT_DIR: String = hdfsTmp + "sort_bam/"
  val UN_SORT_DIR: String = hdfsTmp + "un_sort_bam/"
  val MARK_DUPLICATES_DIR: String = hdfsTmp + "mark_duplicates_bam/"
  val MARK_DUPLICATES_METRICS_DIR: String = hdfsTmp + "mark_duplicates_metrics/"
  val INDEL_REALIGNMENT_DIR: String = hdfsTmp + "indel_realignment_bam/"
  val BASE_RECALIBRATOR_TABLE: String = hdfsTmp + "base_recalibrator_table/"
  val PRINT_READS_DIR: String = hdfsTmp + "print_reads_bam/"
  val MUTECT2_DIR: String = hdfsTmp + "vcf/"

  val dict: SAMSequenceDictionary = SGeneConf.getSequenceDictionary(conf)
  val header: SAMFileHeader = new SAMFileHeader()
  header.setSequenceDictionary(dict)

  val splitBed: Boolean = false
  val keep: Boolean = false
  val upload: Boolean = false

  if (readGroupIdSet.isEmpty || readGroupIdSet.length > 2) throw new Exception("Please specify one or two read group information")

  def variantCallFirstHalf(unsortedSamRecords: Iterable[MySAMRecord]): (Int, String, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)

    val samRecordsListOne = unsortedSamRecords.filter(_.RGID == readGroupIdSet(0)).toArray
    val samRecordsListTwo = unsortedSamRecords.filter(_.RGID == readGroupIdSet(1)).toArray

    Logger.INFOTIME("##### Filter case and normal reads end #####")

    // Write the sorted sam records to disk in bam format
    val sortedBamFileOne = writeSortedBamFile(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    val sortedBamFileTwo = writeSortedBamFile(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    Logger.INFOTIME("##### Write sort bam file end #####")

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), sortedBamFileTwo)

    Logger.INFOTIME("##### MarkDuplicates end #####")

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    Logger.INFOTIME("##### IndelRealignment end #####")

    // Run baseRecalibrator
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    Logger.INFOTIME("##### BaseRecalibrator end #####")
    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallFirstHalfSamtoolsSort(unsortedSamRecords: Iterable[MySAMRecord]): (Int, String, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)
    val tools = new PreprocessTools(bin, conf)

    val samRecordsListOne = unsortedSamRecords.filter(_.RGID == readGroupIdSet(0)).toArray
    val samRecordsListTwo = unsortedSamRecords.filter(_.RGID == readGroupIdSet(1)).toArray

    val unsortedBamOne = writeUnSortedBamFile(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    val unsortedBamTwo = writeUnSortedBamFile(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    val sortedBamFileOne = tmpFileBase + "-" + SGeneConf.getReadGroup(conf, readGroupIdSet(0)).RGID + "-sorted.bam"
    val sortedBamFileTwo = tmpFileBase + "-" + SGeneConf.getReadGroup(conf, readGroupIdSet(1)).RGID + "-sorted.bam"

    tools.runSortBamSamtools(unsortedBamOne, sortedBamFileOne, 6)
    tools.runSortBamSamtools(unsortedBamTwo, sortedBamFileTwo, 6)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), sortedBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallSecondHalf(inputBamFileOne: String, inputBamFileTwo: String, inputTableOne: String, inputTableTwo: String): Unit = {
    Logger.INFOTIME("Processing chromosome region [second]: " + regionId)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last
    val localInputTableOne = localTmp + inputTableOne.split("/").last
    val localInputTableTwo = localTmp + inputTableTwo.split("/").last

    SGeneFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    SGeneFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)
    SGeneFileUtils.downloadFileFromHdfs(inputTableOne, localInputTableOne, delete = false)
    SGeneFileUtils.downloadFileFromHdfs(inputTableTwo, localInputTableTwo, delete = false)

    Logger.INFOTIME("##### Download bam file from HDFS end #####")

    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Run printReads
    val printReadsFileOne = printReads(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), localInputBamFileOne, localInputTableOne)
    val printReadsFileTwo = printReads(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), localInputBamFileTwo, localInputTableTwo)

    Logger.INFOTIME("##### PrintReads end #####")

    // Run mutect2
    val vcfOutputFile = mutect2(printReadsFileOne, printReadsFileTwo)

    Logger.INFOTIME("##### Mutect2 end #####")

    if (new File(vcfOutputFile).exists()) {
      writeVCFOutputFile(vcfOutputFile)
    }
  }

  def wholeGenomeVariantCall(unsortedSamRecords: Iterable[MySAMRecord]): Unit = {
    Logger.INFOTIME("Processing chromosome region [whole genome]: " + regionId)

    val samRecordsListOne = unsortedSamRecords.filter(_.RGID == readGroupIdSet(0)).toArray
    val samRecordsListTwo = unsortedSamRecords.filter(_.RGID == readGroupIdSet(1)).toArray

    scala.util.Sorting.quickSort(samRecordsListOne)
    scala.util.Sorting.quickSort(samRecordsListTwo)

    // Write the sorted sam records to disk in bam format
    val sortedBamFileOne = writeSortedBamFile(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), samRecordsListOne)
    val sortedBamFileTwo = writeSortedBamFile(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), samRecordsListTwo)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), sortedBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), sortedBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseQualityScoreRecalibration
    val baseQualityScoreRecalibrationBamFileOne = baseQualityScoreRecalibration(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    val baseQualityScoreRecalibrationBamFileTwo = baseQualityScoreRecalibration(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    // Run mutect2
    val vcfOutputFile = mutect2(baseQualityScoreRecalibrationBamFileOne, baseQualityScoreRecalibrationBamFileTwo)

    writeVCFOutputFile(vcfOutputFile)
  }

  def variantCallFromMarkDuplicatesFirstHalf(inputBamFileOne: String, inputBamFileTwo: String): (Int, String, String) = {
    Logger.INFOTIME("Processing chromosome region [first]: " + regionId)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    SGeneFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    SGeneFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Build bam index
    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Mark duplicates of the sorted bam file
    val markDuplicatesBamFileOne = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), localInputBamFileOne)
    val markDuplicatesBamFileTwo = markDuplicates(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), localInputBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(markDuplicatesBamFileOne, markDuplicatesBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallFromIndelRealignmentFirstHalf(inputBamFileOne: String, inputBamFileTwo: String): (Int, String, String) = {
    Logger.INFOTIME("[1] " + inputBamFileOne + "   [2] " + inputBamFileTwo)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    SGeneFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    SGeneFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Build bam index
    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Run indelRealignment
    val indelRealignmentOutSet = indelRealignment(localInputBamFileOne, localInputBamFileTwo)
    val indelRealignmentBamFileOne = indelRealignmentOutSet._1
    val indelRealignmentBamFileTwo = indelRealignmentOutSet._2

    // Run baseRecalibrator
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), indelRealignmentBamFileOne)
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), indelRealignmentBamFileTwo)

    (regionId, indelRealignmentOutSet._3, indelRealignmentOutSet._4)
  }

  def variantCallFromBQSRFirstHalf(inputBamFileOne: String, inputBamFileTwo: String): (Int, String, String) = {
    Logger.INFOTIME("[1] " + inputBamFileOne + "   [2] " + inputBamFileTwo)

    val tools = new PreprocessTools(bin, conf)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    SGeneFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    SGeneFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Build bam index
    tools.runBuildBamIndexPicard(localInputBamFileOne)
    tools.runBuildBamIndexPicard(localInputBamFileTwo)

    // Run baseRecalibrator
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(0)), localInputBamFileOne)
    baseRecalibrator(SGeneConf.getReadGroup(conf, readGroupIdSet(1)), localInputBamFileTwo)

    (regionId, inputBamFileOne, inputBamFileTwo)
  }

  def variantCallFromMutect2(inputBamFileOne: String, inputBamFileTwo: String): Unit = {
    Logger.INFOTIME("[1] " + inputBamFileOne + "   [2] " + inputBamFileTwo)

    val localInputBamFileOne = localTmp + inputBamFileOne.split("/").last
    val localInputBamFileTwo = localTmp + inputBamFileTwo.split("/").last

    SGeneFileUtils.downloadFileFromHdfs(inputBamFileOne, localInputBamFileOne)
    SGeneFileUtils.downloadFileFromHdfs(inputBamFileTwo, localInputBamFileTwo)

    // Run mutect2
    val vcfOutputFile = mutect2(localInputBamFileOne, localInputBamFileTwo)

    if (new File(vcfOutputFile).exists()) {
      writeVCFOutputFile(vcfOutputFile)
    }
  }

  def writeSortedBamFile(rg: ReadGroup, sortedSamRecords: Array[MySAMRecord]): String = {
    SGeneFileUtils.mkHdfsDir(SORT_DIR, delete = false)

    val factory: SAMFileWriterFactory = new SAMFileWriterFactory()
    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    outHeader.addReadGroup(rg.getSAMReadGroupRecord())

    val sortOutFile = tmpFileBase + "-" + rg.RGID + "-sorted.bam"
    val hdfsSortOutFile = SORT_DIR + sortOutFile.split("/").last

    val writer: SAMFileWriter = factory.makeBAMWriter(outHeader, true, new File(sortOutFile))
    val samRecordFactory: SAMRecordFactory = new DefaultSAMRecordFactory()
    val validationStringency: ValidationStringency = ValidationStringency.LENIENT
    val parser: SAMLineParser = new SAMLineParser(samRecordFactory, validationStringency, outHeader, null, null)

    for (mySamRecord <- sortedSamRecords) {
      val sam = parser.parseLine(new String(mySamRecord.originalStrByteArr))
      writer.addAlignment(sam)
    }

    writer.close()

    SGeneFileUtils.uploadFileToHdfs(sortOutFile, hdfsSortOutFile, upload)

    sortOutFile
  }

  def readStream(rg: Int, samRecord: Iterator[String]): List[(Int, MySAMRecord)] = {
    var samRecordList: List[(Int, MySAMRecord)] = Nil

    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    outHeader.addReadGroup(SGeneConf.getReadGroup(conf, readGroupIdSet(rg)).getSAMReadGroupRecord())
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
        val chr = if (referenceIndex >= 0 && referenceIndex < ProgramVariable.CHR_NUM) referenceIndex + 1 else ProgramVariable.OTHER_CHR_INDEX
        samRecordList = (chr, new MySAMRecord(sam, itr.getBytes, mateReference = true)) :: samRecordList
      }
    }
    samRecordList
  }

  def writeUnSortedBamFile(rg: ReadGroup, sortedSamRecords: Array[MySAMRecord]): String = {
    SGeneFileUtils.mkHdfsDir(UN_SORT_DIR, delete = false)

    val factory: SAMFileWriterFactory = new SAMFileWriterFactory()
    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.unsorted)
    outHeader.addReadGroup(rg.getSAMReadGroupRecord())

    val unSortOutFile = tmpFileBase + "-" + rg.RGID + "-unsorted.bam"
    val hdfsUnSortOutFile = UN_SORT_DIR + unSortOutFile.split("/").last

    val writer: SAMFileWriter = factory.makeBAMWriter(outHeader, true, new File(unSortOutFile))
    val samRecordFactory: SAMRecordFactory = new DefaultSAMRecordFactory()
    val validationStringency: ValidationStringency = ValidationStringency.LENIENT
    val parser: SAMLineParser = new SAMLineParser(samRecordFactory, validationStringency, outHeader, null, null)

    for (mySamRecord <- sortedSamRecords) {
      val sam = parser.parseLine(new String(mySamRecord.originalStrByteArr))
      writer.addAlignment(sam)
    }

    writer.close()

    SGeneFileUtils.uploadFileToHdfs(unSortOutFile, hdfsUnSortOutFile, upload)

    SGeneFileUtils.deleteLocalFile(unSortOutFile, keep)

    unSortOutFile
  }

  def markDuplicates(rg: ReadGroup, inputBamFile: String): String = {
    SGeneFileUtils.mkHdfsDir(MARK_DUPLICATES_DIR, delete = false)

    val tools = new PreprocessTools(bin, conf)

    val markDuplicatesOutFile = tmpFileBase + "-" + rg.RGID + "-markDuplicates.bam"
    val hdfsMarkDuplicatesOutFile = MARK_DUPLICATES_DIR + markDuplicatesOutFile.split("/").last

    val markDuplicatesMetricsFile = tmpFileBase + "-" + rg.RGID + "-metrics.txt"
    val hdfsMarkDuplicatesMetricsFile = MARK_DUPLICATES_METRICS_DIR + markDuplicatesMetricsFile.split("/").last

    tools.runMarkDuplicates(inputBamFile, markDuplicatesOutFile, markDuplicatesMetricsFile)

    // Generate the bai file of the bam file
    tools.runBuildBamIndexPicard(markDuplicatesOutFile)

    SGeneFileUtils.uploadFileToHdfs(markDuplicatesOutFile, hdfsMarkDuplicatesOutFile, upload)
    SGeneFileUtils.uploadFileToHdfs(markDuplicatesMetricsFile, hdfsMarkDuplicatesMetricsFile, upload)

    SGeneFileUtils.deleteLocalFile(inputBamFile, keep)
    SGeneFileUtils.deleteLocalFile(markDuplicatesMetricsFile, keep)

    markDuplicatesOutFile
  }

  def indelRealignment(inputBamFileOne: String, inputBamFileTwo: String): (String, String, String, String) = {
    SGeneFileUtils.mkHdfsDir(INDEL_REALIGNMENT_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val targetsFile = tmpFileBase + ".intervals"
    val nWayOut = "_realign.bam"

    val outBamFileOne = inputBamFileOne.split("/").last.split('.').head + nWayOut
    val hdfsOutBamFileOne = INDEL_REALIGNMENT_DIR + outBamFileOne

    val outBamFileTwo = inputBamFileTwo.split("/").last.split('.').head + nWayOut
    val hdfsOutBamFileTwo = INDEL_REALIGNMENT_DIR + outBamFileTwo

    val bed =
      if (splitBed) {
        if (SGeneConf.getTargetBedChr(conf).contains(chrId)) {
          SGeneFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          SGeneFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runRealignerTargetCreator(inputBamFileOne, inputBamFileTwo, targetsFile, index, bed)

    gatk.runIndelRealigner(inputBamFileOne, inputBamFileTwo, targetsFile, nWayOut, index)

    SGeneFileUtils.uploadFileToHdfs(outBamFileOne, hdfsOutBamFileOne)
    SGeneFileUtils.uploadFileToHdfs(outBamFileTwo, hdfsOutBamFileTwo)

    SGeneFileUtils.deleteLocalFile(inputBamFileOne, keep)
    SGeneFileUtils.deleteLocalFile(inputBamFileOne.split('.').head + ".bai", keep)
    SGeneFileUtils.deleteLocalFile(inputBamFileTwo, keep)
    SGeneFileUtils.deleteLocalFile(inputBamFileTwo.split('.').head + ".bai", keep)
    SGeneFileUtils.deleteLocalFile(targetsFile, keep)

    (outBamFileOne, outBamFileTwo, hdfsOutBamFileOne, hdfsOutBamFileTwo)
  }

  def baseQualityScoreRecalibration(rg: ReadGroup, inputBamFile: String): String = {
    SGeneFileUtils.mkHdfsDir(PRINT_READS_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val tableFile = tmpFileBase + "-" + rg.RGID + ".table"
    val outBamFile = tmpFileBase + "-" + rg.RGID + "-printreads.bam"
    val hdfsOutBamFile = PRINT_READS_DIR + outBamFile.split("/").last

    val bed =
      if (splitBed) {
        if (SGeneConf.getTargetBedChr(conf).contains(chrId)) {
          SGeneFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          SGeneFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runBaseRecalibrator(inputBamFile, tableFile, index, bed)
    gatk.runPrintReads(inputBamFile, outBamFile, index, tableFile)

    SGeneFileUtils.uploadFileToHdfs(outBamFile, hdfsOutBamFile, upload)

    SGeneFileUtils.deleteLocalFile(inputBamFile, keep)

    outBamFile
  }

  def baseRecalibrator(rg: ReadGroup, inputBamFile: String): Unit = {
    SGeneFileUtils.mkHdfsDir(BASE_RECALIBRATOR_TABLE, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val tableFile = tmpFileBase + "-" + rg.RGID + ".table"
    val hdfsTableFile = BASE_RECALIBRATOR_TABLE + tableFile.split("/").last

    val bed =
      if (splitBed) {
        if (SGeneConf.getTargetBedChr(conf).contains(chrId)) {
          SGeneFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
          localTmp + chrId + ".bed"
        } else {
          SGeneFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
          localTmp + "empty.bed"
        }
      } else {
        ""
      }

    gatk.runBaseRecalibrator(inputBamFile, tableFile, index, bed)

    SGeneFileUtils.deleteLocalFile(inputBamFile, keep)
    SGeneFileUtils.deleteLocalFile(inputBamFile.split('.').head + ".bai", keep)

    SGeneFileUtils.uploadFileToHdfs(tableFile, hdfsTableFile)
    SGeneFileUtils.deleteLocalFile(tableFile, keep)
  }

  def printReads(rg: ReadGroup, inputBamFile: String, table: String): String = {
    SGeneFileUtils.mkHdfsDir(PRINT_READS_DIR, delete = false)

    val gatk = new GATKTools(index, bin, conf)

    val outBamFile = tmpFileBase + "-" + rg.RGID + "-printreads.bam"
    val hdfsOutBamFile = PRINT_READS_DIR + outBamFile.split("/").last

    gatk.runPrintReads(inputBamFile, outBamFile, index, table)

    SGeneFileUtils.uploadFileToHdfs(outBamFile, hdfsOutBamFile, upload)

    SGeneFileUtils.deleteLocalFile(inputBamFile, keep)
    SGeneFileUtils.deleteLocalFile(inputBamFile.split('.').head + ".bai", keep)

    outBamFile
  }

  def mutect2(inputBamFileOne: String, inputBamFileTwo: String): String = {
    val tools = new PreprocessTools(bin, conf)
    val gatk = new GATKTools(index, bin, conf)

    val vcfOutFile = tmpFileBase + ".vcf"

    tools.runBuildBamIndexPicard(inputBamFileOne)
    tools.runBuildBamIndexPicard(inputBamFileTwo)

//    val bed =
//      if (useSplitTargetBed) {
//        if (NGSSparkConf.getTargetBedChr(conf).contains(chrId)) {
//          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + chrId + ".bed", localTmp + chrId + ".bed")
//          localTmp + chrId + ".bed"
//        } else {
//          NGSSparkFileUtils.downloadFileFromHdfs(TARGET_BED_DIR + "empty.bed", localTmp + "empty.bed")
//          localTmp + "empty.bed"
//        }
//      } else {
//        ""
//      }

    val bed =
      if (chrId <= 23 && chrId >= 0) {
        ChromosomeTools.chromosomeTools.getRefNameByRefIndex(chrId - 1)
      } else {
        "/home/spark/GATK/otherChr.list"
      }
    gatk.runMuTect2(inputBamFileOne, inputBamFileTwo, vcfOutFile, index, bed)

    SGeneFileUtils.deleteLocalFile(inputBamFileOne, keep)
    SGeneFileUtils.deleteLocalFile(inputBamFileOne.split('.').head + ".bai", keep)
    SGeneFileUtils.deleteLocalFile(inputBamFileTwo, keep)
    SGeneFileUtils.deleteLocalFile(inputBamFileTwo.split('.').head + ".bai", keep)

    vcfOutFile
  }

  def writeVCFOutputFile(vcfOutputFile: String): Unit = {
    SGeneFileUtils.mkHdfsDir(MUTECT2_DIR, delete = false)

    if (vcfOutputFile != "" && checkVcfIsNotEmpty(vcfOutputFile)) {
      try {
        val hdfsVcfOutputFile = MUTECT2_DIR + vcfOutputFile.split("/").last
        SGeneFileUtils.uploadFileToHdfs(vcfOutputFile, hdfsVcfOutputFile)
        SGeneFileUtils.uploadFileToHdfs(vcfOutputFile + ".idx", hdfsVcfOutputFile + ".idx")
      } catch {
        case e: URISyntaxException =>
          Logger.EXCEPTION(e)
          throw new InterruptedException
      }
    } else if (vcfOutputFile != "") Logger.DEBUG("empty vcf file, not uploaded to vcf to avoid error when merging.")
  }

  def checkVcfIsNotEmpty(vcfFile: String): Boolean = Source.fromFile(vcfFile).getLines.exists(!_.startsWith("#"))
}
