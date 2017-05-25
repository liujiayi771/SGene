package edu.hust.elwg.utils

import java.security.Key
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

class DesUtils(private val defaultSecretKey: String) {
  private val key: Key = getKey(defaultSecretKey.getBytes())
  private val encryptCipher: Cipher = Cipher.getInstance("DES")
  encryptCipher.init(Cipher.ENCRYPT_MODE, key)
  private val decryptCipher: Cipher = Cipher.getInstance("DES")
  decryptCipher.init(Cipher.DECRYPT_MODE, key)

  private def getKey(arrBTmp: Array[Byte]): Key = {
    val arrB: Array[Byte] = new Array[Byte](8)
    for (i <- arrB.indices) {
      arrB(i) = arrBTmp(i)
    }
    new SecretKeySpec(arrB, "DES")
  }

  def encrypt(strIn: String): String = {
    byteArr2HexStr(encrypt(strIn.getBytes()))
  }

  def encrypt(arrB: Array[Byte]): Array[Byte] = {
    encryptCipher.doFinal(arrB)
  }

  def decrypt(strIn: String): String = {
    new String(decrypt(hexStr2ByteArr(strIn)))
  }

  def decrypt(arrB: Array[Byte]): Array[Byte] = {
    decryptCipher.doFinal(arrB)
  }

  def byteArr2HexStr(arrB: Array[Byte]): String = {
    val iLen = arrB.length
    val sb = new StringBuffer(iLen * 2)
    for (i <- 0 until iLen) {
      var intTmp = arrB(i).toInt
      while (intTmp < 0) {
        intTmp = intTmp + 256
      }
      if (intTmp < 16) {
        sb.append("0")
      }
      sb.append(Integer.toString(intTmp, 16))
    }
    sb.toString
  }

  def hexStr2ByteArr(strIn: String): Array[Byte] = {
    val arrB = strIn.getBytes()
    val iLen = arrB.length
    val arrOut: Array[Byte] = new Array[Byte](iLen / 2)
    for (i <- Range(0, iLen, 2)) {
      val strTmp = new String(arrB, i, 2)
      arrOut(i / 2) = Integer.parseInt(strTmp, 16).toByte
    }
    arrOut
  }
}
