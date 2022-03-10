import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.nec.ve.VectorizedRDD._

object RDDBench {

  var sc: SparkContext = null
  var timings: Map[String, Long] = Map()

  def main(args: Array[String]): Unit = {
    println("RDD Benchmark")

    setup()

    benchmark("01 - CPU",  bench01cpu)
    benchmark("01 - VectorEnigne (wip)", bench01ve)

    dumpResult()
    finishUp()
  }

  def benchmark(title: String, f: () => Unit) = {
    val ts_start = System.currentTimeMillis()
    f()
    val ts_end = System.currentTimeMillis()
    val diff = ts_end - ts_start
    timings = timings + (title -> diff)
  }

  def dumpResult() = {
    println("======== RESULTS ===========")
    for ( (title, duration) <- timings) {
      println(title + "\t" + duration + "ms")
    }
    println("============================")
  }


  def bench01cpu() = {
    val numbers = Array(10, 17, 23, 1, 51, 23, 15, 18, 19, 22, 12, 38, 17)
    val rdd = sc.parallelize(numbers)

    val mappedRdd = rdd.map( (a) => 2 * a + 12)

    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench01 is " + result)

  }

  def bench01ve() = {
    val numbers = Array(10, 17, 23, 1, 51, 23, 15, 18, 19, 22, 12, 38, 17)

    val rdd = sc.parallelize(numbers)
    val mappedRdd = rdd.veMap( (a) => 2 * a + 12)

    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench01 is " + result)

  }


  def bench02() = {

    // TODO: read from local file
  /*
    //val lines = sc.textFile("rddbench/data/small.csv")
    val numbersDF = sc.read.csv("rddbench/data/small.csv").rdd

    // map
    val mappedNumbersRDD = numbersRDD.map((a,b) => )

    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a,b) => a+b)
    println("totalLength = " + totalLength)


   */
  }

  def setup(): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDBench")
      .setMaster("yarn")
   //   .set("spark.submit.deployMode", "cluster")
      .setJars(Array("rddbench/target/scala-2.12/rddbench_2.12-0.1.jar"))
    sc = new SparkContext(conf)
  }

  def finishUp() = {
    sc.stop()
  }




}
