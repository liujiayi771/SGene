package tools

import htsjdk.samtools.SAMRecord

class MySAMRecord(originalSamRecord: SAMRecord, val originalStr: String, val normalCase: String) extends Serializable {
  val referenceIndex: Int = originalSamRecord.getReferenceIndex.toInt
  val startPos: Int = originalSamRecord.getAlignmentStart
  val readLen: Int = originalSamRecord.getReadLength
}
