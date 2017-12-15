package edu.hust.elwg.tools

import htsjdk.samtools.SAMSequenceDictionary

class ChromosomeTools(val dict: SAMSequenceDictionary) {
  def chrLen(referenceIndex: Int): Int = {
    dict.getSequence(referenceIndex).getSequenceLength
  }

  def getRefIndexByRefName(name: String): Int = {
    dict.getSequenceIndex(name)
  }

  def getRefNameByRefIndex(index: Int): String = {
    dict.getSequence(index).getSequenceName
  }
}

object ChromosomeTools {
  var chromosomeTools: ChromosomeTools = _

  def apply(dict: SAMSequenceDictionary): ChromosomeTools = {
    if (chromosomeTools == null) {
      chromosomeTools = new ChromosomeTools(dict)
    }
    chromosomeTools
  }
}
