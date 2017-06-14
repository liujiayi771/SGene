package edu.hust.elwg.utils

import java.io.{File, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object NGSSparkFileUtils {
  val RETRIES: Int = 3

  def deleteHdfsFile(path: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val deletePath = new Path(path)
      if (fs.exists(deletePath)) fs.delete(deletePath, true)
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to delete " + path + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
      }
    } finally {
      fs.close()
    }

  }

  def deleteLocalFile(path: String, keep: Boolean): Unit = {
    if (!keep) {
      val deleteFile = new File(path)
      deleteFile.deleteOnExit()
    }
  }

  def deleteLocalDir(dir: File): Unit = {
    if (dir.exists()) {
      val files = dir.listFiles()
      for (file <- files) {
        if (file.isDirectory) {
          deleteLocalDir(file)
        } else {
          file.delete()
        }
      }
      dir.delete()
    }
  }

  def deleteLocalDir(path: String, keep: Boolean): Unit = {
    if (!keep) {
      val f = new File(path)
      deleteLocalDir(f)
    }
  }

  def mkHdfsDir(path: String, delete: Boolean): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val mkdirPath = new Path(path)
    if (fs.exists(mkdirPath)) {
      if (delete) {
        Logger.DEBUG("HDFS Directory " + path + " exists and it will be replaced")
        fs.delete(mkdirPath, true)
        fs.mkdirs(mkdirPath)
      }
    } else {
      fs.mkdirs(mkdirPath)
    }
    fs.close()
  }

  def mkLocalDir(path: String, delete: Boolean): Unit = {
    val localDir = new File(path)
    if (localDir.exists()) {
      if (delete) {
        Logger.DEBUG("Directory " + path + " exists and it will be replaced")
        deleteLocalDir(localDir)
        localDir.mkdirs()
      }
    } else {
      localDir.mkdirs()
    }
  }

  def privateDownloadFileFromHdfs(from: String, to: String): Int = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val f = new File(to)
      if (!f.exists()) {
        fs.copyToLocalFile(new Path(from), new Path(to))
        0
      }
      else {
        Logger.DEBUG("File " + to + " exists and it will be replaced")
        f.delete()
        fs.copyToLocalFile(new Path(from), new Path(to))
        0
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to download " + from + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
        -1
      }
    } finally {
      fs.close()
    }
  }

  def attemptDownloadFileFromHdfs(from: String, to: String, tries: Int): Unit = {
    var res = privateDownloadFileFromHdfs(from, to)
    var try_num = 1
    while (res != 0 && try_num < tries) {
      res = privateDownloadFileFromHdfs(from, to)
      try_num += 1
    }
    if (res == 0) Logger.DEBUG(from + " download")
    else {
      Logger.DEBUG(from + " failed to download")
      throw new IOException()
    }
  }

  def downloadFileFromHdfs(from: String, to: String): Unit = {
    attemptDownloadFileFromHdfs(from, to, RETRIES)
  }

  def privateDownloadFileFromHdfs(from: String, to: String, delete: Boolean): Int = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val f = new File(to)
      if (!f.exists()) {
        fs.copyToLocalFile(new Path(from), new Path(to))
        0
      }
      else {
        if (delete) {
          Logger.DEBUG("File " + to + " exists and it will be replaced")
          f.delete()
          fs.copyToLocalFile(new Path(from), new Path(to))
          0
        } else {
          Logger.DEBUG("File " + to + " exists and don not need download again")
          0
        }
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to download " + from + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
        -1
      }
    } finally {
      fs.close()
    }
  }

  def attemptDownloadFileFromHdfs(from: String, to: String, delete: Boolean, tries: Int): Unit = {
    var res = privateDownloadFileFromHdfs(from, to, delete)
    var try_num = 1
    while (res != 0 && try_num < tries) {
      res = privateDownloadFileFromHdfs(from, to, delete)
      try_num += 1
    }
    if (res == 0) Logger.DEBUG(from + " download")
    else {
      Logger.DEBUG(from + " failed to download")
      throw new IOException()
    }
  }

  def downloadFileFromHdfs(from: String, to: String, delete: Boolean): Unit = {
    attemptDownloadFileFromHdfs(from, to, delete, RETRIES)
  }

  def privateDownloadDirFromHdfs(from: String, to: String): Int = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val f = new File(to)
      val fileStatus = fs.listStatus(new Path(from))
      if (!f.exists()) {
        f.mkdirs()
        for (status <- fileStatus) {
          fs.copyToLocalFile(status.getPath, new Path(to))
        }
        0
      } else {
        Logger.DEBUG("Directory " + to + " exists and it will be replaced")
        deleteLocalDir(f)
        f.mkdirs()
        for (status <- fileStatus) {
          fs.copyToLocalFile(status.getPath, new Path(to))
        }
        0
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to download " + from + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
        -1
      }
    } finally {
        fs.close()
    }
  }

  def attemptDownloadDirFromHdfs(from: String, to: String, tries: Int): Unit = {
    var res = privateDownloadDirFromHdfs(from, to)
    var try_num = 1
    while (res != 0 && try_num < tries) {
      res = privateDownloadDirFromHdfs(from, to)
      try_num += 1
    }
    if (res == 0) Logger.DEBUG(from + " download")
    else {
      Logger.DEBUG(from + " failed to download")
      throw new IOException()
    }
  }

  def downloadDirFromHdfs(from: String, to: String): Unit = {
    attemptDownloadDirFromHdfs(from, to, RETRIES)
  }

  def privateUploadFileToHdfs(from: String, to: String): Int = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val toPath = new Path(to)
      val fromPath = new Path(from)
      if (!fs.exists(toPath)) {
        fs.copyFromLocalFile(fromPath, toPath)
        0
      } else {
        Logger.DEBUG("File " + to + " exists and it will be replaced")
        fs.delete(toPath, true)
        fs.copyFromLocalFile(fromPath, toPath)
        0
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to upload " + from + " to HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
        -1
      }
    } finally {
      fs.close()
    }
  }

  def attemptUploadFileToHdfs(from: String, to: String, tries: Int): Unit = {
    var res = privateUploadFileToHdfs(from, to)
    var try_num = 1
    while (res != 0 && try_num < tries) {
      res = privateUploadFileToHdfs(from, to)
      try_num += 1
    }
    if (res == 0) Logger.DEBUG(from + " uploaded")
    else {
      Logger.DEBUG(from + " failed to upload")
      throw new IOException()
    }
  }

  def uploadFileToHdfs(from: String, to: String): Unit = {
    attemptUploadFileToHdfs(from, to, RETRIES)
  }

  def privateUploadFileToHdfs(from: String, to: String, upload: Boolean): Int = {
    if (upload) {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      try {
        val toPath = new Path(to)
        val fromPath = new Path(from)
        if (!fs.exists(toPath)) {
          fs.copyFromLocalFile(fromPath, toPath)
          return 0
        } else {
          Logger.DEBUG("File " + to + " exists and it will be replaced")
          fs.delete(toPath, true)
          fs.copyFromLocalFile(fromPath, toPath)
          return 0
        }
      } catch {
        case e: IOException => {
          Logger.DEBUG("Failed to upload " + from + " to HDFS: " + e.getLocalizedMessage)
          Logger.EXCEPTION(e)
          return -1
        }
      } finally {
        fs.close()
      }
    }
    0
  }

  def attemptUploadFileToHdfs(from: String, to: String, upload: Boolean, tries: Int): Unit = {
    var res = privateUploadFileToHdfs(from, to, upload)
    var try_num = 1
    while (res != 0 && try_num < tries) {
      res = privateUploadFileToHdfs(from, to, upload)
      try_num += 1
    }
    if (res == 0) Logger.DEBUG(from + " uploaded")
    else {
      Logger.DEBUG(from + " failed to upload")
      throw new IOException()
    }
  }

  def uploadFileToHdfs(from: String, to: String, upload: Boolean): Unit = {
    attemptUploadFileToHdfs(from, to, upload, RETRIES)
  }

  def privateUploadDirToHdfs(from: String, to: String): Int = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val toPath = new Path(to)
      val fromPath = new Path(from)
      if (!fs.exists(toPath)) {
        fs.copyFromLocalFile(fromPath, toPath)
        0
      } else {
        Logger.DEBUG("Directory " + to + " exists and it will be replaced")
        fs.delete(toPath, true)
        fs.copyFromLocalFile(fromPath, toPath)
        0
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to upload " + from + " to HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
        -1
      }
    } finally {
      fs.close()
    }
  }

  def attemptUploadDirToHdfs(from: String, to: String, tries: Int): Unit = {
    var res = privateUploadDirToHdfs(from, to)
    var try_num = 1
    while (res != 0 && try_num < tries) {
      res = privateUploadDirToHdfs(from, to)
      try_num += 1
    }
    if (res == 0) Logger.DEBUG(from + " uploaded")
    else {
      Logger.DEBUG(from + " failed to upload")
      throw new IOException()
    }
  }

  def uploadDirToHdfs(from: String, to: String): Unit = {
    attemptUploadDirToHdfs(from, to, RETRIES)
  }

  def copyFileInHdfs(from: String, to: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      if (fs.exists(new Path(from))) {
        FileUtil.copy(fs, new Path(from), fs, new Path(to), false, conf)
      }
    } finally {
      fs.close()
    }
  }
}
