import org.apache.spark.{SparkConf, SparkContext}
import tools.MySAMRecord
import utils.CommandLine

object NGSSpark {
  val conf: SparkConf = new SparkConf().setAppName("NGS-Spark")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[MySAMRecord]))
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    CommandLine.parseParam(args, conf)

    sc.stop()
  }
}
