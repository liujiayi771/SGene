package edu.hust.elwg.tools

import org.apache.spark.SparkConf
import edu.hust.elwg.utils.NGSSparkConf

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

class PreprocessTools(val bin: String, conf: SparkConf) {
  val java: ArrayBuffer[String] = ArrayBuffer("java")
  val javaCustomArgs: String = NGSSparkConf.getCustomArgs(conf, "java", "")
  if (javaCustomArgs != "") {
    java += javaCustomArgs
  }

  def setJava(java: String): Unit = this.java.update(0, java)

  def runConcatVcf(inputs: Array[String], output: String): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + "vcf-concat" else bin + "/" + "vcf-concat"
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    command += tool
    for (input <- inputs) {
      command += input
    }
    command += ">"
    command += output
    command.mkString(" ").!
  }

  def runBuildBamIndexSamtools(input: String): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + "samtools" else bin + "/" + "samtools"
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    command += tool
    command += "index"
    command += input
    val customArgs: String = NGSSparkConf.getCustomArgs(conf, "samtools", "index")
    val samtoolsBuildBamIndexCmd: Array[String] = CommandGenerator.addToCommand(command.toArray, customArgs)
    samtoolsBuildBamIndexCmd.mkString(" ").!
  }

  def runMergeBamFileSamtools(inputs: Array[String], output: String): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + "samtools" else bin + "/" + "samtools"
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    command += tool
    command += "merge"
    command += output
    for (input <- inputs) {
      command += input
    }
    val customArgs: String = NGSSparkConf.getCustomArgs(conf, "samtools", "merge")
    val samtoolsMergeBamFileCmd: Array[String] = CommandGenerator.addToCommand(command.toArray, customArgs)
    samtoolsMergeBamFileCmd.mkString(" ").!
  }

  val PicardTools: Array[String] = Array(
    "BuildBamIndex.jar",
    "AddOrReplaceReadGroups.jar",
    "MarkDuplicates.jar",
    "CleanSam.jar",
    "picard.jar"
  )

  def runBuildBamIndexPicard(input: String): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + PicardTools(0) else bin + "/" + PicardTools(0)
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = NGSSparkConf.getCustomArgs(conf, "java", "buildbamindex")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += tool
    command += ("INPUT=" + input)
    val customArgs: String = NGSSparkConf.getCustomArgs(conf, "picard", "buildbamindex")
    val picardBuildBamIndexCmd: Array[String] = CommandGenerator.addToCommand(command.toArray, customArgs)
    picardBuildBamIndexCmd.mkString(" ").!
  }

  def runMarkDuplicates(input: String, output: String, metrics: String, keepDups: Boolean): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + PicardTools(4) else bin + "/" + PicardTools(4)
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = NGSSparkConf.getCustomArgs(conf, "java", "markduplicates")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += tool
    command += "MarkDuplicates"
    command += ("INPUT=" + input)
    command += ("OUTPUT=" + output)
    command += ("METRICS_FILE=" + metrics)
    command += "ASO=coordinate"
    command += "OPTICAL_DUPLICATE_PIXEL_DISTANCE=100"
    command += "VALIDATION_STRINGENCY=LENIENT"
    if (!keepDups) command += "REMOVE_DUPLICATES=true"
    val customArgs: String = NGSSparkConf.getCustomArgs(conf, "picard", "markduplicates")
    val picardMarkduplicatesCmd: Array[String] = CommandGenerator.addToCommand(command.toArray, customArgs)
    picardMarkduplicatesCmd.mkString(" ").!
  }

  def runSortVcf(inputs: Array[String], output: String): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + PicardTools(4) else bin + "/" + PicardTools(4)
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = NGSSparkConf.getCustomArgs(conf, "java", "sortvcf")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += tool
    command += "SortVcf"
    for (input <- inputs) {
      command += ("I=" + input)
    }
    command += ("O=" + output)
    val customArgs: String = NGSSparkConf.getCustomArgs(conf, "picard", "sortvcf")
    val picardVcfSortCmd: Array[String] = CommandGenerator.addToCommand(command.toArray, customArgs)
    picardVcfSortCmd.mkString(" ").!
  }

  def runSortBamSamtools(input: String, output: String, threads: Int): Unit = {
    val tool: String = if (bin.endsWith("/")) bin + "samtools" else bin + "/" + "samtools"
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    command += tool
    command += "sort"
    command += ("-@ " + threads.toString)
    command += "-m 1536M"
    command += input
    command += ("-o " + output)
    command.mkString(" ").!
  }
}
