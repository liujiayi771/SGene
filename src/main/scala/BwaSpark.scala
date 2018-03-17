import java.io.{BufferedWriter, InputStream, OutputStreamWriter}

import tools.{CommandGenerator, MySAMRecord}
import utils.{Logger, SGeneConf, SystemShutdownHookRegister}
import htsjdk.samtools._
import htsjdk.samtools.util.BufferedLineReader
import org.apache.spark.SparkConf

import scala.sys.process._

class BwaSpark(settings: Array[(String, String)]) {

  val conf = new SparkConf()
  conf.setAll(settings)

  val OTHER_CHR_INDEX: Int = SGeneConf.getOtherChrIndex(conf)

  val bin: String = SGeneConf.getBin(conf)
  val index: String = SGeneConf.getIndex(conf)
  val localTmp: String = SGeneConf.getLocalTmp(conf)
  val BWAThreads: Int = SGeneConf.getBWAThreads(conf)
  val keepChrSplitPairs: Boolean = SGeneConf.getKeepChrSplitPairs(conf)
  val readGroupIdSet: Array[String] = SGeneConf.getReadGroupId(conf)
  val useLocalCProgram: Boolean = SGeneConf.getUseLocalCProgram(conf)
  val customArgs: String = SGeneConf.getCustomArgs(conf, "bwa", "")

  if (readGroupIdSet.isEmpty || readGroupIdSet.length > 2) throw new Exception("Please specify one or two read group information")

  def runBwaDownloadFile(fileName: String): List[(Int, MySAMRecord)] = {
    val readGroupId = readGroupIdSet(0)
    val readGroup = SGeneConf.getReadGroup(conf, readGroupId).getBwaReadGroup()
    val cmd = CommandGenerator.bwaMem(bin, index, fileName.split("file:")(1), null, isPaired = true, useSTDIN = false, BWAThreads, readGroup, useLocalCProgram, customArgs).mkString(" ")
    Logger.INFOTIME("Run command: " + cmd)
    //    val samRecords = cmd.!!
    //    val samRecordList = readSamStream(fileName, new ByteArrayInputStream(samRecords.getBytes))
    var samRecordList: List[(Int, MySAMRecord)] = Nil
    val io = new ProcessIO(
      in => {},
      out => {
        samRecordList = readSamStream(fileName, out)
        out.close()
      },
      err => {
        scala.io.Source.fromInputStream(err).getLines.foreach(System.err.println)
        err.close()
      }
    )
    val process = cmd.run(io)
    SystemShutdownHookRegister.register(
      "bwa",
      () => {
        process.destroy
      }
    )
    process.exitValue
    samRecordList
  }

  def runBwaStdin(fileName: String, input: String): List[(Int, MySAMRecord)] = {
    val readGroupId = if (fileName.contains(readGroupIdSet(0))) readGroupIdSet(0) else readGroupIdSet(1)
    val readGroup = SGeneConf.getReadGroup(conf, readGroupId).getBwaReadGroup()

    val cmd = CommandGenerator.bwaMem(bin, index, null, null, isPaired = true, useSTDIN = true, BWAThreads, readGroup, useLocalCProgram, customArgs).mkString(" ")
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
    val process = cmd.run(io)
    SystemShutdownHookRegister.register(
      "bwa",
      () => {
        process.destroy
      }
    )
    process.exitValue
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
      val read1Ref = samRecord.getReferenceIndex.toInt
      val read2Ref = samRecord.getMateReferenceIndex.toInt
      if (!samRecord.getReadUnmappedFlag &&
        (read1Ref == read2Ref || keepChrSplitPairs) &&
        (read1Ref >= 0 || read2Ref >= 0)) {
        if (read1Ref >= 0) {
          val chr = if (read1Ref < SGeneConf.getChromosomeNum(conf)) read1Ref + 1 else OTHER_CHR_INDEX
          samRecordList = (chr, new MySAMRecord(samRecord, mCurrentLine, mateReference = false)) :: samRecordList
        }
        //        if (read2Ref >= 0 && read2Ref < NGSSparkConf.getChromosomeNum(conf) && read1Ref != read2Ref) {
        //          val chr = if (read2Ref < NGSSparkConf.getChromosomeNum(conf)) read2Ref + 1 else OTHER_CHR_INDEX
        //          samRecordList = (chr, new MySAMRecord(samRecord, mCurrentLine, mateReference = true)) :: samRecordList
        //        }
      }
      mCurrentLine = mReader.readLine()
    }
    Logger.INFOTIME("Sam stream counts " + count + " records")
    samRecordList
  }


}
