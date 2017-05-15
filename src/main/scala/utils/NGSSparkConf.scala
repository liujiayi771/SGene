package utils

import htsjdk.samtools.{SAMSequenceDictionary, SAMSequenceRecord}
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import tools.ReadGroup

/**
  * Created by joey on 2017/5/15.
  */
object NGSSparkConf {
  /** ------------------------------------------------ Boolean param --------------------------------------------- **/

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
    for (i <- 0 to count) {
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

  /** ------------------------------------------------ Arguments ------------------------------------------------- **/

  val partitionNum = "partitionnum"

  def setPartitionNum(conf: SparkConf, value: Int): Unit = conf.set(partitionNum, value.toString)

  def getPartitionNum(conf: SparkConf): Int = conf.get(partitionNum).toInt

  /** ------------------------------------------------ Arguments ------------------------------------------------- **/

  val customArgs = "ca_"

  def setCustomArgs(conf: SparkConf, programName: String, toolName: String, value: String): Unit = conf.set(customArgs + programName.toLowerCase + "_" + toolName.toLowerCase, value)

  def getCustomArgs(conf: SparkConf, programName: String, toolName: String): String = conf.get(customArgs + programName.toLowerCase + "_" + toolName.toLowerCase, "")

  /** ------------------------------------------------- Read group information ----------------------------------- **/

  val readGroup = "rg_"

  def setReadGroup(conf: SparkConf, value: ReadGroup): Unit = conf.set(readGroup + value.RGID, value.toString)

  def getReadGroup(conf: SparkConf, RGID: String): String = conf.get(readGroup + RGID)

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

}
