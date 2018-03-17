package tools

import htsjdk.samtools.{SAMRecord, SAMTag}

class MySAMRecord(originalSamRecord: SAMRecord, val originalStrByteArr: Array[Byte], val mateReference: Boolean) extends Serializable {
  val RGID: String = originalSamRecord.getAttribute(SAMTag.RG.name).toString
  val referenceIndex: Int = originalSamRecord.getReferenceIndex.toInt
//  val referenceName: String = originalSamRecord.getReferenceName
  val startPos: Int = originalSamRecord.getAlignmentStart
  val readLen: Int = originalSamRecord.getReadLength
  val mateStartPos: Int = originalSamRecord.getMateAlignmentStart
  var regionId: Int = if (referenceIndex < ProgramVariable.CHR_NUM) referenceIndex + 1 else ProgramVariable.OTHER_CHR_INDEX
  def addRegionId(increase: Int): Unit = {
    regionId += increase
  }
}

object MySAMRecord {
  implicit val samRecordOrdering: Ordering[MySAMRecord] = new Ordering[MySAMRecord] {
    override def compare(x: MySAMRecord, y: MySAMRecord): Int = {
      if (x.referenceIndex != y.referenceIndex) x.referenceIndex - y.referenceIndex
      else {
        if (x.startPos != y.startPos) {
          x.startPos - y.startPos
        } else {
          if (new String(x.originalStrByteArr) > new String(y.originalStrByteArr)) 1 else -1
        }
      }
    }
  }
}
