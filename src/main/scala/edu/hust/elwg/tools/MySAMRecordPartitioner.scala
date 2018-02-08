package edu.hust.elwg.tools

import org.apache.spark.Partitioner

class MySAMRecordPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val record: MySAMRecord = key.asInstanceOf[MySAMRecord]
    val read1Ref = record.referenceIndex
    val chr = if (read1Ref < ProgramVariable.CHR_NUM) read1Ref + 1 else ProgramVariable.OTHER_CHR_INDEX
    val code = chr.hashCode() % numPartitions
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case records: MySAMRecordPartitioner =>
      records.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode(): Int = numPartitions
}
