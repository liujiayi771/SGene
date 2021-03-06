package tools

import java.text.DecimalFormat

import utils.{Logger, SGeneConf, SystemShutdownHookRegister}
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

class GATKTools(val reference: String, val bin: String, conf: SparkConf) {
  val readGroupIdSet: Array[String] = SGeneConf.getReadGroupId(conf)
  val java: ArrayBuffer[String] = ArrayBuffer("java")
  val mem: String = "-Xmx2g"
  val gatk: String = if (bin.endsWith("/")) bin + "gatk-package-distribution-3.6.jar" else bin + "/" + "gatk-package-distribution-3.6.jar"
  var threads: Int = 1
  val onedec: DecimalFormat = new DecimalFormat("###0.0")
  val multiThreadingTypes: Array[String] = Array("-nt", "-nct")

  def setJava(java: String): Unit = this.java.update(0, java)

  def setThreads(threads: Int): Unit = this.threads = threads

  def roundOneDecimal(value: Double): String = onedec.format(value)

  /**
    * Run the RealignerTargetCreator program of GATK using one input file
    *
    * @param input   input bam file
    * @param targets output interval file
    * @param ref     reference file
    */
  def runRealignerTargetCreator(input: String, targets: String, ref: String): Unit = {
    /**
      * example:
      * java -Xmx8g -jar GenomeAnalysisTK.jar \
      * -T RealignerTargetCreator \
      * -R ref.fasta \
      * -I input.bam \
      * -o forIndelRealigner.intervals
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "realignertargetcreator")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T RealignerTargetCreator"
    command += ("-R " + ref)
    command += ("-I " + input)
    command += ("-o " + targets)
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "realignertargetcreator")
    val realignerTargetCreatorCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(realignerTargetCreatorCmd.mkString(" "))
    val process = realignerTargetCreatorCmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "realignertargetcreator",
      () => {
        process.destroy
      }
    )
    process.exitValue
  }

  /**
    * Run the RealignerTargetCreator program of GATK using two input files (case and normal)
    *
    * @param inputOne first input bam file of normal
    * @param inputTwo second input bam file of case
    * @param targets  output interval file
    * @param ref      reference file
    */
  def runRealignerTargetCreator(inputOne: String, inputTwo: String, targets: String, ref: String, bed: String): Unit = {
    /**
      * example:
      * java -Xmx8g -jar GenomeAnalysisTK.jar \
      * -T RealignerTargetCreator \
      * -R ref.fasta \
      * -I input.bam \
      * -o forIndelRealigner.intervals
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "realignertargetcreator")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T RealignerTargetCreator"
    command += ("-R " + ref)
    command += ("-I " + inputOne)
    command += ("-I " + inputTwo)
    command += ("-o " + targets)
    if (bed != "" && bed != null) command += ("-L " + bed)
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "realignertargetcreator")
    val realignerTargetCreatorCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(realignerTargetCreatorCmd.mkString(" "))
    val process = realignerTargetCreatorCmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "realignertargetcreator",
      () => {
        process.destroy
      }
    )
    process.exitValue
  }

  /**
    * Run the IndelRealigner program of GATK using one input file
    *
    * @param input   input bam file
    * @param targets input interval file generated by RealignerTargetCreator
    * @param output  output bam file
    * @param ref     reference file
    */
  def runIndelRealigner(input: String, targets: String, output: String, ref: String): Unit = {
    /**
      * example:
      * java -Xmx4g -jar GenomeAnalysisTK.jar \
      * -T IndelRealigner \
      * -R ref.fasta \
      * -I input.bam \
      * -targetIntervals intervalListFromRTC.intervals \
      * -o realignedBam.bam \
      * [-known /path/to/indels.vcf] \
      * [-compress 0]
      *
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "indelrealigner")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T IndelRealigner"
    command += ("-R " + ref)
    command += ("-I " + input)
    command += ("-targetIntervals " + targets)
    command += ("-o " + output)
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "indelrealigner")
    val indelRealignerCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(indelRealignerCmd.mkString(" "))
    val process = indelRealignerCmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "indelrealigner",
      () => {
        process.destroy
      }
    )
    process.exitValue
  }

  /**
    * Run the IndelRealigner program of GATK using two input file (case and normal)
    *
    * @param inputOne first input bam file
    * @param inputTwo second input bam file of case
    * @param targets  input interval file generated by RealignerTargetCreator
    * @param nWayOut  suffix of the two output bam files
    * @param ref      reference file
    */
  def runIndelRealigner(inputOne: String, inputTwo: String, targets: String, nWayOut: String, ref: String): Unit = {
    /**
      * example:
      * java -Xmx4g -jar GenomeAnalysisTK.jar \
      * -T IndelRealigner \
      * -R ref.fasta \
      * -I input.bam \
      * -targetIntervals intervalListFromRTC.intervals \
      * -o realignedBam.bam \
      * [-known /path/to/indels.vcf] \
      * [-compress 0]
      *
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "indelrealigner")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T IndelRealigner"
    command += ("-R " + ref)
    command += ("-I " + inputOne)
    command += ("-I " + inputTwo)
    command += ("-targetIntervals " + targets)
    command += ("--nWayOut " + nWayOut)
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "indelrealigner")
    val indelRealignerCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(indelRealignerCmd.mkString(" "))
    val process = indelRealignerCmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "indelrealigner",
      () => {
        process.destroy
      }
    )
    process.exitValue
  }

  def runBaseRecalibrator(input: String, table: String, ref: String, bed: String): Unit = {
    /**
      * example: from CountCovariates
      * -I input.bam -T Countcovariates -R ref -knownSites dbsnp
      * -cov ReadGroupCovariate -cov QualityScoreCovariate -cov DinucCovariate
      * -cov HomopolymerCovariate
      * -recalFile recal.csv
      *
      * java -Xmx4g -jar GenomeAnalysisTK.jar \
      * -T BaseRecalibrator \
      * -I my_reads.bam \
      * -R resources/Homo_sapiens_assembly18.fasta \
      * -knownSites bundle/hg18/dbsnp_132.hg18.vcf \
      * -knownSites another/optional/setOfSitesToMask.vcf \
      * -o recal_data.table
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "baserecalibrator")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T BaseRecalibrator"
    command += ("-R " + ref)
    command += ("-I " + input)
    command += ("-o " + table)
    if (bed != "" && bed != null) command += ("-L " + bed)
    command += "--disable_auto_index_creation_and_locking_when_reading_rods"
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "baserecalibrator")
    val baseRecalibratorCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(baseRecalibratorCmd.mkString(" "))
    val process = baseRecalibratorCmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "baserecalibrator",
      () => {
        process.destroy
      }
    )
    process.exitValue
  }

  def runPrintReads(input: String, output: String, ref: String, table: String): Unit = {
    /**
      * example:
      * java -Xmx8g -Xms8g -jar GenomeAnalysisTK.jar \
      * -T PrintReads \
      * -R ref.fasta \
      * -I input.bam \
      * -o recalibrated.bam \
      * -BQSR table.grp
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "printreads")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T PrintReads"
    command += ("-R " + ref)
    command += ("-I " + input)
    command += ("-o " + output)
    command += ("-BQSR " + table)
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "printreads")
    val printReadsCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(printReadsCmd.mkString(" "))
    val process = printReadsCmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "printreads",
      () => {
        process.destroy
      }
    )
    process.exitValue
  }

  def runMuTect2(inputOne: String, inputTwo: String, output: String, ref: String, bed: String): Unit = {
    /**
      * example:
      * java -Xmx8g -Xms8g -jar GenomeAnalysisTK.jar \
      * -T MuTect2 \
      * -R ref.fasta \
      * -I:tumor case.bam \
      * -I:normal normal.bam \
      * -o output.vcf \
      * -L target.bed \
      * --dnsp dbsnp_138.b37.vcf.gz \
      * --cosmic b37_cosmic_v73_061615.vcf.gz
      * -contamination 0 \
      * --max_alt_alleles_in_normal_count 3 \
      * --max_alt_alleles_in_normal_qscore_sum 40 \
      * --max_alt_alleles_in_normal_fraction 0.02 \
      * -dt NONE \
      */
    val normalFile = if (readGroupIdSet(0) == "normal" || readGroupIdSet(0) == "control") inputOne else inputTwo
    val tumorFile = if (readGroupIdSet(0) == "tumor" || readGroupIdSet(0) == "case") inputOne else inputTwo

    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "mutect2")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T MuTect2"
    command += ("-R " + ref)
    command += ("-I:tumor " + tumorFile)
    command += ("-I:normal " + normalFile)
    command += ("-o " + output)
    if (bed != "" && bed != null) command += ("-L " + bed)
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "mutect2")
    val mutect2Cmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(mutect2Cmd.mkString(" "))
    val process = mutect2Cmd.mkString(" ").run
    SystemShutdownHookRegister.register(
      "mutect2",
      () => {
        process.destroy
      }
    )
//    sys addShutdownHook {
//      Logger.INFO("Caught shutdown, kill process")
//      process.destroy
//    }
    process.exitValue
  }

  def runHaplotypeCaller(input: String, output: String, ref: String, scc: Double, sec: Double): Unit = {
    /**
      * example:
      * java -Xmx8g -Xms8g -jar GenomeAnalysisTK.jar \
      * -T UnifiedGenotyper \
      * -I recalibrated.bam \
      * -o output.vcf \
      * -R ref \
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "variantcaller")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T HaplotypeCaller"
    command += ("-R " + ref)
    command += ("-I " + input)
    command += ("-o " + output)
    command += ("-stand_call_conf " + roundOneDecimal(scc))
    command += ("-stand_emit_conf " + roundOneDecimal(sec))
    command += "--no_cmdline_in_header"
    command += "--disable_auto_index_creation_and_locking_when_reading_rods"
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "variantcaller")
    val haplotypeCallerCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(haplotypeCallerCmd.mkString(" "))
    val process = haplotypeCallerCmd.mkString(" ").run
    sys addShutdownHook {
      Logger.INFO("Caught shutdown, kill process")
      process.destroy
    }
    process.exitValue
  }

  def runCombineVariants(inputs: Array[String], output: String, ref: String): Unit = {
    /**
      * java -Xmx2g -jar GenomeAnalysisTK.jar \
      * -R ref.fasta \
      * -T CombineVariants \
      * --variant input1.vcf \
      * --variant input2.vcf \
      * -o output.vcf \
      * -genotypeMergeOptions UNIQUIFY
      */
    val command: ArrayBuffer[String] = ArrayBuffer.empty
    val javaCustomArgs: String = SGeneConf.getCustomArgs(conf, "java", "combinevariants")
    command ++= java
    command += javaCustomArgs
    command += "-jar"
    command += gatk
    command += "-T CombineVariants"
    command += ("-R " + ref)
    command += ("-o " + output)
    command += "-sites_only"
    command += "-genotypeMergeOptions"
    command += "UNIQUIFY"
    command += (multiThreadingTypes(0) + " " + threads)
    if (inputs != null) {
      for (in <- inputs) {
        command += ("--variant " + in)
      }
    }
    val customArgs: String = SGeneConf.getCustomArgs(conf, "gatk", "combinevariants")
    val combineVariantsCmd = CommandGenerator.addToCommand(command.toArray, customArgs)
    Logger.INFOTIME(combineVariantsCmd.mkString(" "))
    val process = combineVariantsCmd.mkString(" ").run
    sys addShutdownHook {
      Logger.INFO("Caught shutdown, kill process")
      process.destroy
    }
    process.exitValue
  }
}
