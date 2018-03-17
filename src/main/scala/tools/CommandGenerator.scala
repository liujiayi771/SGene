package tools

import scala.collection.mutable.ArrayBuffer

object CommandGenerator {
  def addToCommand(command: Array[String], args: String): Array[String] = {
    if (args == null || args.isEmpty) return command
    (command.toList ::: args.split("\\s+").toList).toArray
  }

  def randomString(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString

  val bwaCommand: Array[String] = Array("bwa", "samxe")
  val bwaTool: Array[String] = Array("mem", "aln")
  val bwaOption: Array[String] = Array(
    "-p", // 0: paired (interleaved file)
    "-t", // 1: number of threads for bwa program
    "-R",  // 2: readGroup information
    "-P", // 3: perform SW to rescue missing hits only but do not try to find hits that fit a proper pair
    "-M" // 4: mark shorter split hits as secondary (for Picard compatibility)
  )

  def bwaMem(
              bin: String, // location of the bwa program file
              bwaReferenceIndex: String, // location of the reference index file
              bwaReadsFile1: String, // location of the first input fastq file
              bwaReadsFile2: String, // location of the second input fastq file
              isPaired: Boolean, // whether paired input (interleaved file)
              useSTDIN: Boolean, // whether use /dev/stdin for input stream
              numberOfThreads: Int, // number of threads for bwa program
              readGroup: String,
              useLocalCProgram: Boolean,
              customArgs: String // other arguments for bwa program
            ): Array[String] = {
    val command: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (useLocalCProgram) command += "bwa" else command += (bin + bwaCommand(0))
    command += bwaTool(0)
    command += bwaReferenceIndex
    if (isPaired) command += bwaOption(0)
    command += bwaOption(1)
    command += numberOfThreads.toString
    command += bwaOption(3)
    command += bwaOption(4)
    command += bwaOption(2)
    command += readGroup
    if (useSTDIN) {
      command += "/dev/stdin"
    } else {
      command += bwaReadsFile1
      if (!isPaired && bwaReadsFile2 != null)
        command += bwaReadsFile2
    }

    addToCommand(command.toArray, customArgs)
  }
}

