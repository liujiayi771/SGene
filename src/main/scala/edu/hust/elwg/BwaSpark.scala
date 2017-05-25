package edu.hust.elwg

import java.io.{BufferedWriter, ByteArrayInputStream, InputStream, OutputStreamWriter}

import edu.hust.elwg.tools.{CommandGenerator, MySAMRecord}
import edu.hust.elwg.utils.{Logger, NGSSparkConf, NGSSparkFileUtils}
import htsjdk.samtools._
import htsjdk.samtools.util.BufferedLineReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf

import scala.sys.process._

class BwaSpark(settings: Array[(String, String)]) {

  val conf = new SparkConf()
  conf.setAll(settings)

  val OTHER_CHR_INDEX: Int = NGSSparkConf.getOtherChrIndex(conf)

  val bin: String = NGSSparkConf.getBin(conf)
  val index: String = NGSSparkConf.getIndex(conf)
  val localTmp: String = NGSSparkConf.getLocalTmp(conf)
  val BWAThreads: Int = NGSSparkConf.getBWAThreads(conf)
  val keepChrSplitPairs: Boolean = NGSSparkConf.getKeepChrSplitPairs(conf)
  val readGroupIdSet: Array[String] = NGSSparkConf.getReadGroupId(conf)
  val customArgs: String = NGSSparkConf.getCustomArgs(conf, "bwa", "")

  if (readGroupIdSet.isEmpty || readGroupIdSet.length > 2) throw new Exception("Please specify one or two read group information")

  def runBwaDownloadFile(fileName: String): List[(Int, MySAMRecord)] = {
    val readGroupId = if (fileName.contains(readGroupIdSet(0))) readGroupIdSet(0) else readGroupIdSet(1)
    val downloadChunkFile = localTmp + readGroupId + "-" + fileName.split("/").last
    val readGroup = NGSSparkConf.getReadGroup(conf, readGroupId).getBwaReadGroup()

    NGSSparkFileUtils.downloadFileFromHdfs(fileName, downloadChunkFile)
    val cmd = CommandGenerator.bwaMem(bin, index, downloadChunkFile, null, isPaired = true, useSTDIN = false, BWAThreads, readGroup, customArgs).mkString(" ")
    Logger.INFOTIME("Run command: " + cmd)
    val samRecords = cmd.!!
    val samRecordList = readSamStream(fileName, new ByteArrayInputStream(samRecords.getBytes))
    samRecordList
  }

  def runBwaStdin(fileName: String, input: String): List[(Int, MySAMRecord)] = {
    val readGroupId = if (fileName.contains(readGroupIdSet(0))) readGroupIdSet(0) else readGroupIdSet(1)
    val readGroup = NGSSparkConf.getReadGroup(conf, readGroupId).getBwaReadGroup()

    val cmd = CommandGenerator.bwaMem(bin, index, null, null, isPaired = true, useSTDIN = true, BWAThreads, readGroup, customArgs).mkString(" ")
    Logger.INFOTIME("Run command: " + cmd)
    var samRecordList: List[(Int, MySAMRecord)] = Nil
    val io = new ProcessIO(
      in => {
        val stdin = new BufferedWriter(new OutputStreamWriter(in))
        stdin.write(input, 0, input.length)
        stdin.newLine()
        stdin.close()
        in.close()
      },
      out => {
        samRecordList = readSamStream(fileName, out)
        out.close()
      },
      err => {
        scala.io.Source.fromInputStream(err).getLines.foreach(System.err.println)
        err.close()
      }
    )
    cmd.run(io).exitValue()
    samRecordList
  }

  def readSamStream(fileName: String, stdout: InputStream): List[(Int, MySAMRecord)] = {
    var samRecordList: List[(Int, MySAMRecord)] = Nil

    val validationStringency: ValidationStringency = ValidationStringency.LENIENT
    val samRecordFactory: SAMRecordFactory = new DefaultSAMRecordFactory()
    val mReader = new BufferedLineReader(stdout)
    val headerCodec: SAMTextHeaderCodec = new SAMTextHeaderCodec()
    headerCodec.setValidationStringency(validationStringency)
    val mFileHeader = headerCodec.decode(mReader, null)
    val parser = new SAMLineParser(samRecordFactory, validationStringency, mFileHeader, null, null)

    var count: Int = 0
    var mCurrentLine = mReader.readLine()
    while (mCurrentLine != null) {
      count += 1
      val samRecord: SAMRecord = parser.parseLine(mCurrentLine, mReader.getLineNumber)
      val referenceIndex: Int = samRecord.getReferenceIndex.toInt
      val read1Ref = samRecord.getReferenceIndex
      val read2Ref = samRecord.getMateReferenceIndex
      if (!samRecord.getReadUnmappedFlag &&
        (read1Ref == read2Ref || keepChrSplitPairs) &&
        (read1Ref >= 0 || read2Ref >= 0)) {
        val chr = if (referenceIndex >= 0 && referenceIndex < NGSSparkConf.getChromosomeNum(conf)) referenceIndex + 1 else OTHER_CHR_INDEX
        samRecordList = (chr, new MySAMRecord(samRecord, mCurrentLine)) :: samRecordList
      }
      mCurrentLine = mReader.readLine()
    }
    Logger.INFOTIME("Sam stream counts " + count + " records")
    samRecordList
  }


}
