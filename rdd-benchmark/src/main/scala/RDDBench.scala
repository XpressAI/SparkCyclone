import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

object RDDBench {

  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    println("RDD Benchmark")

    setupLocal()

    val lines = sc.textFile("rdd-benchmark/data.txt")
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a,b) => a+b)

    println("totalLength = " + totalLength)
  }

  def setupLocal(): Unit = {
    val conf = new SparkConf().setAppName("RDDBench").setMaster("yarn")
    sc = new SparkContext(conf)
  }


}
