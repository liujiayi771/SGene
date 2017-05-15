package tools

import htsjdk.samtools.SAMReadGroupRecord

class ReadGroup(readGroupStr: String) {
  var RGID = ""
  var RGLB = ""
  var RGSM = ""
  var RGPU = ""
  var RGPL = ""

  val elements: Array[String] = readGroupStr.split(" ")
  for (ele <- elements) {
    val value: Array[String] = ele.split(":")
    value(0).toLowerCase match {
      case "id" => RGID = value(1)
      case "lb" => RGLB = value(1)
      case "sm" => RGSM = value(1)
      case "pu" => RGPU = value(1)
      case "pl" => RGPL = value(1)
    }
  }

  def getSAMReadGroupRecord(): SAMReadGroupRecord = {
    val bamrg: SAMReadGroupRecord = new SAMReadGroupRecord(RGID)
    bamrg.setLibrary(RGLB)
    bamrg.setPlatform(RGPL)
    bamrg.setPlatformUnit(RGPU)
    bamrg.setSample(RGSM)
    bamrg
  }

  override def toString: String = {
    s"ID:$RGID LB:$RGLB SM:$RGSM PU:$RGPU PL:$RGPL"
  }
}
