package tools

import org.apache.spark.Partitioner

class MySAMRecordPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val record: MySAMRecord = key.asInstanceOf[MySAMRecord]

    val code = record.regionId % numPartitions
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
