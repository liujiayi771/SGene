
import java.io.IOException

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tools.MySAMRecord
import utils.{CommandLine, Logger, NGSSparkConf}

object NGSSpark {
  val conf: SparkConf = new SparkConf().setAppName("NGS-Spark")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[MySAMRecord]))
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    CommandLine.parseParam(args, conf)
    val partitionNum = NGSSparkConf.getPartitionNum(conf)
    val inputDirs = NGSSparkConf.getInput(conf)

    val inputChunkFileRDD: RDD[(String, String)] = if (inputDirs.length == 1) {
      sc.wholeTextFiles(inputDirs(0), partitionNum)
    } else if (inputDirs.length == 2) {
      (sc.wholeTextFiles(inputDirs(0)) ++ sc.wholeTextFiles(inputDirs(1))).repartition(partitionNum)
    } else {
      throw new IOException("Please specify one or two input directory")
    }

    inputChunkFileRDD.map(itr => itr._1).collect.foreach(println)
    sc.stop()
  }
}
