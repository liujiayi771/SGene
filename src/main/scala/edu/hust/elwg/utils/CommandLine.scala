package edu.hust.elwg.utils

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.net.URISyntaxException

import edu.hust.elwg.tools.{ChromosomeTools, ReadGroup}
import htsjdk.samtools.{SAMSequenceDictionary, SAMSequenceRecord}
import org.apache.spark.SparkConf
import org.sellmerfud.optparse.{OptionParser, OptionParserException}

import scala.io.Source

object CommandLine {

  case class Param(
                    /** Boolean param */
                    drop: Boolean = true,

                    /** Files param */
                    bin: String = "",
                    index: String = "",
                    input: List[String] = Nil,
                    localTmp: String = "",
                    hdfsTmp: String = "",
                    targetBed: String = "",
                    output: String = "",

                    /** Threads and partitions */
                    partitionNum: Int = 0,
                    BWAThreads: Int = 0,

                    /** Arguments */
                    customArgs: List[String] = Nil,

                    /** Read group information */
                    readGroup: List[String] = Nil,

                    /** Others */
                    useLocalCProgram: Boolean = false,
                    useSplitTargetBed: Boolean = false
                  )

  private val DICT_SUFFIX: String = ".dict"
  private var param: Param = _

  def parseParam(args: Array[String], conf: SparkConf): Unit = {
    param = try {
      new OptionParser[Param] {
        banner = "NGS-Spark [options] file..."
        separator("")
        separator("Options:")

        /** ------------------------------------------------ Boolean param --------------------------------------------- **/
        bool("", "--drop", "We will drop all paired-end reads where the pairs are aligned to different chromosomes.") { (v, c) => c.copy(drop = !v) }

        /** ------------------------------------------------ Files param ----------------------------------------------- **/
        reqd[String]("-B", "--bin", "Binary location. This string gives the location where bin is located.") { (v, c) => c.copy(bin = v) }
        reqd[String]("", "--index <indexFile>", "Specify the path of index file.") { (v, c) => c.copy(index = v) }
        reqd[String]("-I", "--input", "Input directory. The string point to the directory containing the preprocessed input on the used file system.") { (v, c) => c.copy(input = c.input :+ v) }
        reqd[String]("", "--local_tmp <STR>", "Temporary directory. This string gives the location where intermediate files will be stored. " +
          "This should be on a local disk for every node for optimal performance.") { (v, c) => c.copy(localTmp = v) }
        reqd[String]("", "--hdfs_tmp <STR>", "Temporary directory in HDFS.") { (v, c) => c.copy(hdfsTmp = v) }
        reqd[String]("", "--bed <STR>", "Target bed file for variant calling.") { (v, c) => c.copy(targetBed = v) }
        reqd[String]("", "--output <STR>", "The name of output file.") { (v, c) => c.copy(output = v) }

        /** ------------------------------------------------ Threads and partitions ------------------------------------ **/
        reqd[Int]("-n <partition number>", "", "Enter the number of partition.") { (v, c) => c.copy(partitionNum = v) }
        reqd[Int]("-t", "--bwathreads <bwaThreadsNum>", "Specify the number of threads run in BWA.") { (v, c) => c.copy(BWAThreads = v) }

        /** ------------------------------------------------ Arguments ------------------------------------------------- **/
        reqd[String]("", "--CA <STR==STR>", "Custom arguments. This options allows the edu.hust.elwg.tools.tools to run with additional arguments. " +
          "The arguments are given in this form: toolname==extra arguments.") { (v, c) => c.copy(customArgs = c.customArgs :+ v) }

        /** ------------------------------------------------- Read group information ----------------------------------- **/
        reqd[String]("", "--read_group", "Read Group information.") { (v, c) => c.copy(readGroup = c.readGroup :+ v) }

        /** ------------------------------------------------ Others ---------------------------------------------------- **/
        bool("", "--use_local_c_program", "Use local bwa and samtool program.") { (v, c) => c.copy(useLocalCProgram = v) }
        bool("", "--split_target_bed", "Use split targetBed in Mutect2.") { (v, c) => c.copy(useSplitTargetBed = v) }

      }.parse(args, Param())
    } catch {
      case e: OptionParserException => println(e.getMessage); sys.exit(1)
    }

    setSparkConf(conf)
  }

  private def setSparkConf(conf: SparkConf): Unit = {
    /** Boolean param */
    NGSSparkConf.setKeepChrSplitPairs(conf, param.drop)

    /** Files param */
    NGSSparkConf.setBin(conf, param.bin)
    NGSSparkConf.setIndex(conf, param.index)
    NGSSparkConf.setInput(conf, param.input)
    NGSSparkConf.setLocalTmp(conf, param.localTmp)
    NGSSparkConf.setHdfsTmp(conf, param.hdfsTmp)
    NGSSparkConf.setBedFile(conf, param.targetBed)
    NGSSparkConf.setOutput(conf, param.output)

    /** Threads and partitions */
    NGSSparkConf.setPartitionNum(conf, param.partitionNum)
    NGSSparkConf.setBWAThreads(conf, param.BWAThreads)

    /** Arguments */
    parseCustomArgs(conf)

    /** Read group information */
    for (rg <- param.readGroup) {
      NGSSparkConf.setReadGroup(conf, new ReadGroup(rg))
    }

    /** Others */
    parseDictFile(conf)
    NGSSparkConf.setUseLocalCProgram(conf, param.useLocalCProgram)
    NGSSparkConf.setUseSplitTargetBed(conf, param.useSplitTargetBed)
  }

  private def parseCustomArgs(conf: SparkConf): Unit = {
    Logger.DEBUG("parsing custom arguments")
    for (arg <- param.customArgs) {
      val elements = arg.split("==")
      val programAndTool = elements(0).split('_')
      val programName = programAndTool(0)
      val toolName = if (programAndTool.length == 1) "" else programAndTool(1)
      NGSSparkConf.setCustomArgs(conf, programName, toolName, elements(1))
    }
  }

  private def parseDictFile(conf: SparkConf): Unit = {
    Logger.DEBUG("parsing dictionary " + param.index.substring(0, param.index.indexOf('.')) + DICT_SUFFIX)
    try {
      val dict: SAMSequenceDictionary = new SAMSequenceDictionary()
      val lines = Source.fromFile(param.index.substring(0, param.index.indexOf('.')) + DICT_SUFFIX).getLines()
      for (line <- lines.drop(1)) {
        val lineData: Array[String] = line.split("\\s+")
        val seqName: String = lineData(1).substring(lineData(1).indexOf(':') + 1)
        var seqLength = 0
        try {
          seqLength = lineData(2).substring(lineData(2).indexOf(':') + 1).toInt
        } catch {
          case e: NumberFormatException => Logger.EXCEPTION(e)
        }
        val seq: SAMSequenceRecord = new SAMSequenceRecord(seqName, seqLength)
        dict.addSequence(seq)
      }
      NGSSparkConf.setSequenceDictionary(conf, dict)
    } catch {
      case e: URISyntaxException => Logger.EXCEPTION(e)
      case e: IOException => Logger.EXCEPTION(e)
    }
  }
}
