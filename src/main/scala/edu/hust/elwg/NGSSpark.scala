package edu.hust.elwg


import java.io._

import edu.hust.elwg.tools.{ChromosomeTools, MySAMRecord, PreprocessTools}
import edu.hust.elwg.utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object NGSSpark {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("NGS-Spark").setMaster("local[*]")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "2g")
    conf.registerKryoClasses(Array(classOf[MySAMRecord]))
    val sc: SparkContext = new SparkContext(conf)
    CommandLine.parseParam(args, conf)
    val CHR_NUM = 24
    val OTHER_CHR_INDEX = 99
    NGSSparkConf.setChromosomeNum(conf, CHR_NUM)
    NGSSparkConf.setOtherChrIndex(conf, OTHER_CHR_INDEX)

    val partitionNum = NGSSparkConf.getPartitionNum(conf)
    val inputDirs = NGSSparkConf.getInput(conf)
    val outputPrefix = NGSSparkConf.getOutput(conf)
    val localTmp = NGSSparkConf.getLocalTmp(conf)
    val bin = NGSSparkConf.getBin(conf)
    val readGroupIdSet = NGSSparkConf.getReadGroupId(conf)
    val BASE_RECALIBRATOR_TABLE: String = localTmp + "base_recalibrator_table/"
    val TABLE: String = localTmp + "table/"
    val MUTECT2_DIR: String = localTmp + "vcf/"
    val chrTools = ChromosomeTools(NGSSparkConf.getSequenceDictionary(conf))


    /** clean tmp file **/
//    val workerNum: Int = sc.getConf.get("spark.cores.max").toInt
//    val worker: RDD[Int] = sc.parallelize(Range(0, workerNum), workerNum)
//    worker.map(_ => {
//      NGSSparkFileUtils.mkLocalDir(localTmp, delete = true)
//    }).count()
    NGSSparkFileUtils.mkLocalDir(localTmp, delete = true)

    parseTargetBed(conf)
    val confBC: Broadcast[Array[(String, String)]] = sc.broadcast(conf.getAll)

    val inputChunkFileRDD: RDD[(String, String)] = if (inputDirs.length == 1) {
      sc.wholeTextFiles(inputDirs.head, partitionNum)
    } else if (inputDirs.length == 2) {
      sc.wholeTextFiles(inputDirs.head, partitionNum) ++ sc.wholeTextFiles(inputDirs(1), partitionNum)
    } else {
      throw new IOException("Please specify one or two input directory")
    }

    val allSamRecordsRDD: RDD[(Int, MySAMRecord)] = inputChunkFileRDD.flatMap(itr => {
      val bwa = new BwaSpark(confBC.value)
      bwa.runBwaDownloadFile(itr._1)
    })

    //    val allSamRecordsRDD = sc.textFile("data/normal.sam").mapPartitions(itr => {
    //      val v = new VariantCalling(confBC.value, 0)
    //      v.readStream(0, itr).toIterator
    //    }) ++ sc.textFile("data/case.sam").mapPartitions(itr => {
    //      val v = new VariantCalling(confBC.value, 0)
    //      v.readStream(1, itr).toIterator
    //    })

    allSamRecordsRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    allSamRecordsRDD.count()

    val avgSamRecords: Long = allSamRecordsRDD.count / (CHR_NUM + 1)
    val chrToNumSamRecs: RDD[(Int, Int)] = allSamRecordsRDD.filter(_._1 != OTHER_CHR_INDEX).map(record => (record._1, 1)).reduceByKey(_ + _)
    val chrInfo: Map[Int, (Int, Int)] = chrToNumSamRecs.map(record => {
      val conf: SparkConf = new SparkConf()
      conf.setAll(confBC.value)
      (record._1, (record._2, ChromosomeTools(NGSSparkConf.getSequenceDictionary(conf)).chrLen(record._1)))
    }).collect.toMap

    val allChrToSamRecordsRDD: RDD[(Int, Iterable[MySAMRecord])] = allSamRecordsRDD
      .flatMap(itr => balanceLoad(itr, chrInfo, avgSamRecords, CHR_NUM))
      .groupByKey(CHR_NUM * 2)
//      .filter(itr => NGSSparkConf.getTargetBedChr(conf).contains(itr._1))

    val firstHalf: RDD[(Int, String, String)] = allChrToSamRecordsRDD.sortBy(itr => itr._2.size, ascending = false).map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallFirstHalf(itr._2)
    })
    firstHalf.persist(StorageLevel.MEMORY_ONLY_SER)
    firstHalf.count()

    /** Download table file **/
    val tableFile = new File(BASE_RECALIBRATOR_TABLE)
    val oneFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(0)))
    val twoFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(1)))
    val oneOutputTableFile = localTmp + readGroupIdSet(0) + ".table"

    val twoOutputTableFile = localTmp + readGroupIdSet(1) + ".table"

    MergeTables.mergeTable(oneFile, twoFile, oneOutputTableFile, twoOutputTableFile)

    firstHalf.map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallSecondHalf(itr._2, itr._3, oneOutputTableFile, twoOutputTableFile)
    }).count()

    //    runFromMarkDuplicates(sc, confBC, readGroupIdSet)
    //    runFromMutect2(sc, confBC, readGroupIdSet)
    //    runFromBQSR(sc, confBC, readGroupIdSet)

    /** Download vcf files from HDFS **/
    var vcfFiles = new File(MUTECT2_DIR).listFiles().map(_.getAbsolutePath).filter(_.endsWith(".vcf"))

    // Sort vcf files
    vcfFiles = vcfFiles.sortWith((a, b) => {
      if (chrTools.getRefIndexByRefName(Source.fromFile(a).getLines().filter(!_.startsWith("#")).toArray.head.split("\\s+").head) ==
        chrTools.getRefIndexByRefName(Source.fromFile(b).getLines().filter(!_.startsWith("#")).toArray.head.split("\\s+").head)) {
        a.split("/").last.split('.').head.split("-")(1).toInt < b.split("/").last.split('.').head.split("-")(1).toInt
      } else {
        chrTools.getRefIndexByRefName(Source.fromFile(a).getLines().filter(!_.startsWith("#")).toArray.head.split("\\s+").head) <
          chrTools.getRefIndexByRefName(Source.fromFile(b).getLines().filter(!_.startsWith("#")).toArray.head.split("\\s+").head)
      }
    })
    vcfFiles.foreach(println)
    val tools = new PreprocessTools(bin, conf)

    tools.runSortVcf(vcfFiles, outputPrefix + "-" + Timer.getGlobalDate + ".vcf")

    sc.stop()
  }

  def runFromMutect2(sc: SparkContext, confBC: Broadcast[Array[(String, String)]], rgSet: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAll(confBC.value)

    val data = sc.wholeTextFiles("/user/spark/sparkgatk_L001/print_reads_bam")
      .map(itr => (itr._1.split("/").last.split("-")(1).toInt, itr._1))
    data.groupByKey(NGSSparkConf.getChromosomeNum(conf)).map(itr => {
      var bamFileOne = ""
      var bamFileTwo = ""
      for (bam <- itr._2) {
        if (bam.contains(rgSet(0))) bamFileOne = bam
        if (bam.contains(rgSet(1))) bamFileTwo = bam
      }
      (itr._1, bamFileOne, bamFileTwo)
    }).map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallFromMutect2(itr._2, itr._3)
    }).count()
  }

  def runFromIndelRealignment(sc: SparkContext, confBC: Broadcast[Array[(String, String)]], rgSet: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAll(confBC.value)

    val localTmp = NGSSparkConf.getLocalTmp(conf)
    val hdfsTmp = NGSSparkConf.getHdfsTmp(conf)
    val readGroupIdSet = NGSSparkConf.getReadGroupId(conf)
    val BASE_RECALIBRATOR_TABLE: String = hdfsTmp + "base_recalibrator_table/"
    val TABLE: String = hdfsTmp + "table/"

    val data = sc.wholeTextFiles("/user/spark/sparkgatk_L001/mark_duplicates_bam")
      .map(itr => (itr._1.split("/").last.split("-")(1).toInt, itr._1))
    val firstHalf = data.groupByKey(NGSSparkConf.getChromosomeNum(conf)).map(itr => {
      var bamFileOne = ""
      var bamFileTwo = ""
      for (bam <- itr._2) {
        if (bam.contains(rgSet(0))) bamFileOne = bam
        if (bam.contains(rgSet(1))) bamFileTwo = bam
      }
      (itr._1, bamFileOne, bamFileTwo)
    }).map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallFromIndelRealignmentFirstHalf(itr._2, itr._3)
    })
    firstHalf.persist(StorageLevel.MEMORY_ONLY_SER)
    firstHalf.count()

    /** Download table file **/
    val localTableDir = localTmp + BASE_RECALIBRATOR_TABLE.split("/").last
    NGSSparkFileUtils.downloadDirFromHdfs(BASE_RECALIBRATOR_TABLE, localTableDir)
    val tableFile = new File(localTableDir)
    val oneFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(0)))
    val twoFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(1)))
    val oneOutputTableFile = localTmp + readGroupIdSet(0) + ".table"
    val hdfsOneOutputTableFile = TABLE + oneOutputTableFile.split("/").last
    val twoOutputTableFile = localTmp + readGroupIdSet(1) + ".table"
    val hdfsTwoOutputTableFile = TABLE + twoOutputTableFile.split("/").last
    MergeTables.mergeTable(oneFile, twoFile, oneOutputTableFile, twoOutputTableFile)

    NGSSparkFileUtils.uploadFileToHdfs(oneOutputTableFile, hdfsOneOutputTableFile)
    NGSSparkFileUtils.uploadFileToHdfs(twoOutputTableFile, hdfsTwoOutputTableFile)

    firstHalf.map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallSecondHalf(itr._2, itr._3, hdfsOneOutputTableFile, hdfsTwoOutputTableFile)
    }).count()
  }

  def runFromBQSR(sc: SparkContext, confBC: Broadcast[Array[(String, String)]], rgSet: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAll(confBC.value)

    val localTmp = NGSSparkConf.getLocalTmp(conf)
    val hdfsTmp = NGSSparkConf.getHdfsTmp(conf)
    val readGroupIdSet = NGSSparkConf.getReadGroupId(conf)
    val BASE_RECALIBRATOR_TABLE: String = hdfsTmp + "base_recalibrator_table/"
    val TABLE: String = hdfsTmp + "table/"

    val data = sc.wholeTextFiles("/user/spark/sparkgatk_L001/indel_realignment_bam")
      .map(itr => (itr._1.split("/").last.split("-")(1).toInt, itr._1))
    val firstHalf = data.groupByKey(NGSSparkConf.getChromosomeNum(conf)).map(itr => {
      var bamFileOne = ""
      var bamFileTwo = ""
      for (bam <- itr._2) {
        if (bam.contains(rgSet(0))) bamFileOne = bam
        if (bam.contains(rgSet(1))) bamFileTwo = bam
      }
      (itr._1, bamFileOne, bamFileTwo)
    }).map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallFromBQSRFirstHalf(itr._2, itr._3)
    })
    firstHalf.persist(StorageLevel.MEMORY_ONLY_SER)
    firstHalf.count()

    /** Download table file **/
    val localTableDir = localTmp + BASE_RECALIBRATOR_TABLE.split("/").last
    NGSSparkFileUtils.downloadDirFromHdfs(BASE_RECALIBRATOR_TABLE, localTableDir)
    val tableFile = new File(localTableDir)
    val oneFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(0)))
    val twoFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(1)))
    val oneOutputTableFile = localTmp + readGroupIdSet(0) + ".table"
    val hdfsOneOutputTableFile = TABLE + oneOutputTableFile.split("/").last
    val twoOutputTableFile = localTmp + readGroupIdSet(1) + ".table"
    val hdfsTwoOutputTableFile = TABLE + twoOutputTableFile.split("/").last
    MergeTables.mergeTable(oneFile, twoFile, oneOutputTableFile, twoOutputTableFile)

    NGSSparkFileUtils.uploadFileToHdfs(oneOutputTableFile, hdfsOneOutputTableFile)
    NGSSparkFileUtils.uploadFileToHdfs(twoOutputTableFile, hdfsTwoOutputTableFile)

    firstHalf.map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallSecondHalf(itr._2, itr._3, hdfsOneOutputTableFile, hdfsTwoOutputTableFile)
    }).count()
  }

  def runFromMarkDuplicates(sc: SparkContext, confBC: Broadcast[Array[(String, String)]], rgSet: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAll(confBC.value)

    val localTmp = NGSSparkConf.getLocalTmp(conf)
    val hdfsTmp = NGSSparkConf.getHdfsTmp(conf)
    val readGroupIdSet = NGSSparkConf.getReadGroupId(conf)
    val BASE_RECALIBRATOR_TABLE: String = hdfsTmp + "base_recalibrator_table/"
    val TABLE: String = hdfsTmp + "table/"

    val data = sc.wholeTextFiles("/user/spark/sparkgatk_L001/sort_bam")
      .map(itr => (itr._1.split("/").last.split("-")(1).toInt, itr._1))
    val firstHalf = data.groupByKey(NGSSparkConf.getChromosomeNum(conf)).map(itr => {
      var bamFileOne = ""
      var bamFileTwo = ""
      for (bam <- itr._2) {
        if (bam.contains(rgSet(0))) bamFileOne = bam
        if (bam.contains(rgSet(1))) bamFileTwo = bam
      }
      (itr._1, bamFileOne, bamFileTwo)
    }).map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallFromMarkDuplicatesFirstHalf(itr._2, itr._3)
    })
    firstHalf.persist(StorageLevel.MEMORY_ONLY_SER)
    firstHalf.count()

    /** Download table file **/
    val localTableDir = localTmp + BASE_RECALIBRATOR_TABLE.split("/").last
    NGSSparkFileUtils.downloadDirFromHdfs(BASE_RECALIBRATOR_TABLE, localTableDir)
    val tableFile = new File(localTableDir)
    val oneFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(0)))
    val twoFile = tableFile.listFiles().map(_.getAbsolutePath).filter(name => name.endsWith(".table") && name.contains(readGroupIdSet(1)))
    val oneOutputTableFile = localTmp + readGroupIdSet(0) + ".table"
    val hdfsOneOutputTableFile = TABLE + oneOutputTableFile.split("/").last
    val twoOutputTableFile = localTmp + readGroupIdSet(1) + ".table"
    val hdfsTwoOutputTableFile = TABLE + twoOutputTableFile.split("/").last
    MergeTables.mergeTable(oneFile, twoFile, oneOutputTableFile, twoOutputTableFile)

    NGSSparkFileUtils.uploadFileToHdfs(oneOutputTableFile, hdfsOneOutputTableFile)
    NGSSparkFileUtils.uploadFileToHdfs(twoOutputTableFile, hdfsTwoOutputTableFile)

    firstHalf.map(itr => {
      val vc = new VariantCalling(confBC.value, itr._1)
      vc.variantCallSecondHalf(itr._2, itr._3, hdfsOneOutputTableFile, hdfsTwoOutputTableFile)
    }).count()
  }

  def check(desFile: String, des: DesUtils): Int = {
    if (!new File(desFile).exists()) throw new Exception("Timestamp not exist error")
    val mmSplits = des.decrypt(Source.fromFile(desFile).mkString).split('$')
    if (mmSplits(0) != "###" || mmSplits(2) != "###") throw new Exception("Timestamp error")
    mmSplits(1).toInt
  }

  /**
    * Use some strategy to regroup the sam records in order to realize load balancing.
    * If the reference index of a sam record is in the range of normal human chromosome (1-24),
    * we check whether the number of sam records belong to this chromosome is larger than
    * (1.5 * average number of sam records for all chromosome), we split the sam records belong to
    * this chromosome into two parts according to the mid point of the chromosome.
    * If the start alignment of the sam record is smaller than half of the chromosome length, we don't do
    * anything, otherwise we change the key of the RDD record to (key + number of chromosome).
    * Beside, if the mid point of the chromosome if in the range of sam record, we put this sam record
    * to both part.
    *
    * @param record          a single record of the RDD
    * @param chromosomesInfo Information about the chromosomes, about the number of sam records belong to
    *                        this chromosome and the total length of this chromosome
    * @param avgn            Average number of sam records for all chromosome
    * @return Array of regroup sam record, the number of elements in this array is 1 (one part) or 2 (two part)
    */
  def balanceLoad(record: (Int, MySAMRecord), chromosomesInfo: Map[Int, (Int, Int)], avgn: Long, CHR_NUM: Int): Array[(Int, MySAMRecord)] = {

    val output: ArrayBuffer[(Int, MySAMRecord)] = ArrayBuffer.empty
    val limit = avgn * 1.5
    var key = record._1
    val sam = record._2
    if (key >= 1 && key <= CHR_NUM) {
      val chrNum = chromosomesInfo.size
      val chrInfo = chromosomesInfo(key)
      if (chrInfo._1 > limit) {
        val beginPos = if (!sam.mateReference) sam.startPos else sam.mateStartPos
        if (beginPos > (chrInfo._2 / 2)) {
          key = key + chrNum
          output.append((key, sam))
        } else {
          output.append((key, sam))
          val endPos = beginPos + sam.readLen
          if (endPos > (chrInfo._2 / 2)) {
            output.append((key + chrNum, sam))
          }
        }
      } else {
        output.append((key, sam))
      }
    } else {
      output.append((key, sam))
    }
    output.toArray
  }

  def parseTargetBed(conf: SparkConf): Unit = {
    val bedFile = NGSSparkConf.getBedFile(conf)
    val chrTool = ChromosomeTools(NGSSparkConf.getSequenceDictionary(conf))
    val targetBedPath = NGSSparkConf.getLocalTmp(conf) + "targetBed/"
    NGSSparkFileUtils.mkHdfsDir(targetBedPath, delete = false)
    val lines = Source.fromFile(bedFile).getLines
    val groupLines = lines.toList.groupBy(_.split("\\s+").head)
    for (index <- groupLines) {
      NGSSparkConf.setTargetBedChr(conf, chrTool.getRefIndexByRefName(index._1) + 1)
      val localBedFile = targetBedPath + (chrTool.getRefIndexByRefName(index._1) + 1) + ".bed"

      val f = new File(localBedFile)
      val out = new BufferedWriter(new FileWriter(f))
      for (line <- index._2) {
        out.write(line + "\n")
      }
      out.close()
    }
    val f = new File(targetBedPath + "empty.bed")
    f.createNewFile()
  }
}
