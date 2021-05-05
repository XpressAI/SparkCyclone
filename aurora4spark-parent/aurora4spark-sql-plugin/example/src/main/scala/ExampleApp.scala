import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.net.InetAddress

object ExampleApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", "com.nec.spark.LocalVeoExtension")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      println(InetAddress.getLocalHost.getHostName)

      println(sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].head())
    } finally sparkSession.close()
  }

}
