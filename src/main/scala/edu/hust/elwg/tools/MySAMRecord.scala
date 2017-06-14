package edu.hust.elwg.tools

import htsjdk.samtools.{SAMRecord, SAMTag}

class MySAMRecord(originalSamRecord: SAMRecord, val originalStr: String, val mateReference: Boolean) extends Serializable {
  val RGID: String = originalSamRecord.getAttribute(SAMTag.RG.name).toString
  val referenceIndex: Int = originalSamRecord.getReferenceIndex.toInt
  val startPos: Int = originalSamRecord.getAlignmentStart
  val readLen: Int = originalSamRecord.getReadLength
  val mateStartPos: Int = originalSamRecord.getMateAlignmentStart
}
