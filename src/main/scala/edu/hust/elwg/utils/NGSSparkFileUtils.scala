package edu.hust.elwg.utils

import java.io.{File, IOException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object NGSSparkFileUtils {
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

  def downloadFileFromHdfs(from: String, to: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val f = new File(to)
      if (!f.exists()) fs.copyToLocalFile(new Path(from), new Path(to))
      else {
        Logger.DEBUG("File " + to + " exists and it will be replaced")
        f.delete()
        fs.copyToLocalFile(new Path(from), new Path(to))
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to download " + from + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
      }
    } finally {
      fs.close()
    }
  }

  def downloadFileFromHdfs(from: String, to: String, delete: Boolean): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val f = new File(to)
      if (!f.exists()) fs.copyToLocalFile(new Path(from), new Path(to))
      else {
        if (delete) {
          Logger.DEBUG("File " + to + " exists and it will be replaced")
          f.delete()
          fs.copyToLocalFile(new Path(from), new Path(to))
        } else {
          Logger.DEBUG("File " + to + " exists and don not need download again")
        }
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to download " + from + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
      }
    } finally {
      fs.close()
    }
  }

  def downloadDirFromHdfs(from: String, to: String): Unit = {
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
      } else {
        Logger.DEBUG("Directory " + to + " exists and it will be replaced")
        deleteLocalDir(f)
        f.mkdirs()
        for (status <- fileStatus) {
          fs.copyToLocalFile(status.getPath, new Path(to))
        }
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to download " + from + " from HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
      }
    } finally {
        fs.close()
    }
  }

  def uploadFileToHdfs(from: String, to: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val toPath = new Path(to)
      val fromPath = new Path(from)
      if (!fs.exists(toPath)) {
        fs.copyFromLocalFile(fromPath, toPath)
      } else {
        Logger.DEBUG("File " + to + " exists and it will be replaced")
        fs.delete(toPath, true)
        fs.copyFromLocalFile(fromPath, toPath)
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to upload " + from + " to HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
      }
    } finally {
      fs.close()
    }
  }

  def uploadFileToHdfs(from: String, to: String, upload: Boolean): Unit = {
    if (upload) {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      try {
        val toPath = new Path(to)
        val fromPath = new Path(from)
        if (!fs.exists(toPath)) {
          fs.copyFromLocalFile(fromPath, toPath)
        } else {
          Logger.DEBUG("File " + to + " exists and it will be replaced")
          fs.delete(toPath, true)
          fs.copyFromLocalFile(fromPath, toPath)
        }
      } catch {
        case e: IOException => {
          Logger.DEBUG("Failed to upload " + from + " to HDFS: " + e.getLocalizedMessage)
          Logger.EXCEPTION(e)
        }
      } finally {
        fs.close()
      }
    }
  }

  def uploadDirToHdfs(from: String, to: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    try {
      val toPath = new Path(to)
      val fromPath = new Path(from)
      if (!fs.exists(toPath)) {
        fs.copyFromLocalFile(fromPath, toPath)
      } else {
        Logger.DEBUG("Directory " + to + " exists and it will be replaced")
        fs.delete(toPath, true)
        fs.copyFromLocalFile(fromPath, toPath)
      }
    } catch {
      case e: IOException => {
        Logger.DEBUG("Failed to upload " + from + " to HDFS: " + e.getLocalizedMessage)
        Logger.EXCEPTION(e)
      }
    } finally {
      fs.close()
    }
  }
}
