package edu.hust.elwg.utils

import edu.hust.elwg.tools.ReadGroup
import htsjdk.samtools.{SAMSequenceDictionary, SAMSequenceRecord}
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/**
  * Created by joey on 2017/5/15.
  */
object NGSSparkConf {
  /** ------------------------------------------------ Boolean param --------------------------------------------- **/

  val drop = "drop"

  def setKeepChrSplitPairs(conf: SparkConf, value: Boolean): Unit = if (value) conf.set(drop, "true") else conf.set(drop, "false")

  def getKeepChrSplitPairs(conf: SparkConf): Boolean = if (conf.get(drop, "true").equalsIgnoreCase("true")) true else false

  /** ------------------------------------------------ Files param ----------------------------------------------- **/

  val bin = "binpath"

  def setBin(conf: SparkConf, value: String): Unit = if (!value.endsWith("/")) conf.set(bin, value + "/") else conf.set(bin, value)

  def getBin(conf: SparkConf): String = conf.get(bin)

  val index = "indexpath"

  def setIndex(conf: SparkConf, value: String): Unit = conf.set(index, value)

  def getIndex(conf: SparkConf): String = conf.get(index)

  val input = "inputpath_"
  val inputCount = "inputcount"

  def setInput(conf: SparkConf, value: List[String]): Unit = {
    conf.set(inputCount, value.length.toString)
    for (i <- value.indices) if (!value.endsWith("/")) conf.set(input + i, value(i) + "/") else conf.set(input + i, value(i))
  }

  def getInput(conf: SparkConf): List[String] = {
    val count = conf.get(inputCount).toInt
    var inputList: List[String] = Nil
    for (i <- 0 until count) {
      inputList = conf.get(input + i) :: inputList
    }
    inputList
  }

  val localTmp = "localtmp"

  def setLocalTmp(conf: SparkConf, value: String): Unit = if (!value.endsWith("/")) conf.set(localTmp, value + "/") else conf.set(localTmp, value)

  def getLocalTmp(conf: SparkConf): String = conf.get(localTmp)

  val hdfsTmp = "hdfstmp"

  def setHdfsTmp(conf: SparkConf, value: String): Unit = if (!value.endsWith("/")) conf.set(hdfsTmp, value + "/") else conf.set(hdfsTmp, value)

  def getHdfsTmp(conf: SparkConf): String = conf.get(hdfsTmp)

  val bed = "bed"

  def setBedFile(conf: SparkConf, value: String): Unit = conf.set(bed, value)

  def getBedFile(conf: SparkConf): String = conf.get(bed)

  val output = "output"

  def setOutput(conf: SparkConf, value: String): Unit = conf.set(output, value)

  def getOutput(conf: SparkConf): String = conf.get(output)

  /** ------------------------------------------------ Threads and partitions ------------------------------------ **/

  val partitionNum = "partitionnum"

  def setPartitionNum(conf: SparkConf, value: Int): Unit = conf.set(partitionNum, value.toString)

  def getPartitionNum(conf: SparkConf): Int = conf.get(partitionNum).toInt

  val BWAThreads = "bwathreads"

  def setBWAThreads(conf: SparkConf, value: Int): Unit = conf.set(BWAThreads, value.toString)

  def getBWAThreads(conf: SparkConf): Int = conf.get(BWAThreads).toInt

  /** ------------------------------------------------ Arguments ------------------------------------------------- **/

  val customArgs = "ca_"

  def setCustomArgs(conf: SparkConf, programName: String, toolName: String, value: String): Unit = conf.set(customArgs + programName.toLowerCase + "_" + toolName.toLowerCase, value)

  def getCustomArgs(conf: SparkConf, programName: String, toolName: String): String = conf.get(customArgs + programName.toLowerCase + "_" + toolName.toLowerCase, "")

  /** ------------------------------------------------- Read group information ----------------------------------- **/

  val readGroup = "rg_"

  def setReadGroup(conf: SparkConf, value: ReadGroup): Unit = {
    conf.set(readGroup + value.RGID, value.toString)
    setReadGroupId(conf, value.RGID)
  }

  def getReadGroup(conf: SparkConf, RGID: String): ReadGroup = new ReadGroup(conf.get(readGroup + RGID))

  val readGroupId = "rgid"

  def setReadGroupId(conf: SparkConf, value: String): Unit = {
    val rgIdSet = conf.get(readGroupId, "")
    if (rgIdSet == "")
      conf.set(readGroupId, value)
    else
      conf.set(readGroupId, rgIdSet + ":" + value)
  }

  def getReadGroupId(conf: SparkConf): Array[String] = {
    val rgIdSet = conf.get(readGroupId, "")
    rgIdSet.split(':')
  }

  /** ------------------------------------------------ Others ---------------------------------------------------- **/

  val dictionarySequenceName: String = "seqdictionary_"
  val dictionarySequenceLength: String = "seqdictionarylength_"
  val dictionaryCount: String = "seqcount"

  def setSequenceDictionary(conf: SparkConf, dict: SAMSequenceDictionary): Unit = {
    var counter = 0
    val seq = dict.getSequences
    for (i <- Range(0, seq.size)) {
      conf.set(dictionarySequenceName + counter, seq(i).getSequenceName)
      conf.set(dictionarySequenceLength + counter, seq(i).getSequenceLength.toString)
      counter += 1
    }
    conf.set(dictionaryCount, counter.toString)
  }

  def getSequenceDictionary(conf: SparkConf): SAMSequenceDictionary = {
    val counter = conf.get(dictionaryCount, "0").toInt
    val dict: SAMSequenceDictionary = new SAMSequenceDictionary()
    for (i <- Range(0, counter)) {
      val seqName = conf.get(dictionarySequenceName + i)
      val seqLength = conf.get(dictionarySequenceLength + i).toInt
      val seq: SAMSequenceRecord = new SAMSequenceRecord(seqName, seqLength)
      dict.addSequence(seq)
    }
    dict
  }

  val chromosomeNum: String = "chrnum"

  def setChromosomeNum(conf: SparkConf, value: Int): Unit = conf.set(chromosomeNum, value.toString)

  def getChromosomeNum(conf: SparkConf): Int = conf.get(chromosomeNum).toInt

  val otherChrIndex: String = "otherchrindex"

  def setOtherChrIndex(conf: SparkConf, value: Int): Unit = conf.set(otherChrIndex, value.toString)

  def getOtherChrIndex(conf: SparkConf): Int = conf.get(otherChrIndex).toInt

  val useLocalCProgram: String = "uselocalcprogram"

  def setUseLocalCProgram(conf: SparkConf, value: Boolean): Unit = if (value) conf.set(useLocalCProgram, "true") else conf.set(useLocalCProgram, "false")

  def getUseLocalCProgram(conf: SparkConf): Boolean = if (conf.get(useLocalCProgram, "true").equalsIgnoreCase("true")) true else false

  val useSplitTargetBed: String = "usesplittargetbed"

  def setUseSplitTargetBed(conf: SparkConf, value: Boolean): Unit = if (value) conf.set(useSplitTargetBed, "true") else conf.set(useSplitTargetBed, "false")

  def getUseSplitTargetBed(conf: SparkConf): Boolean = if (conf.get(useSplitTargetBed, "true").equalsIgnoreCase("true")) true else false

  val targetBedChrIndex: String = "targetbedchr"

  def setTargetBedChr(conf: SparkConf, value: Int): Unit = {
    val chrIndexSet = conf.get(targetBedChrIndex, "")
    if (chrIndexSet == "")
      conf.set(targetBedChrIndex, value.toString)
    else
      conf.set(targetBedChrIndex, chrIndexSet + ":" + value.toString)
  }

  def getTargetBedChr(conf: SparkConf): Array[Int] = {
    val chrIndexSet = conf.get(targetBedChrIndex, "")
    chrIndexSet.split(':').map(_.toInt)
  }

  val des = "zxcTcguCHiaAaeAS"

  val runExpired: Int = 600000
}
