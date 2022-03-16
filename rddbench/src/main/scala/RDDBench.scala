import com.nec.ve.VectorizedRDD
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object RDDBench {

  var sc: SparkContext = _
  var timings: Map[String, Double] = Map()

  def main(args: Array[String]): Unit = {
    import com.nec.ve.VectorizedRDD.toVectorizedRDD

    println("RDD Benchmark")

    println("setup")
    sc = setup()

    println("Making numbers")
    val numbers = (1 to (1000000)).toArray
    val rdd = sc.parallelize(numbers)

    println("Making VeRDD")
    val verdd = toVectorizedRDD(rdd)

    println("Starting Benchmark")
    benchmark("01 - CPU",  () => bench01cpu(rdd))
    benchmark("01 - VE ", () => bench01ve(verdd))

    dumpResult()
    finishUp()
  }

  def benchmark(title: String, f: () => Unit) = {
    val ts_start = System.nanoTime()
    f()
    val ts_end = System.nanoTime()
    val diff = (ts_end - ts_start) / 1000000.0
    timings = timings + (title -> diff)
  }

  def dumpResult() = {
    println("======== RESULTS ===========")
    for ( (title, duration) <- timings) {
      println(title + "\t" + duration + "seconds")
    }
    println("============================")
  }


  def bench01cpu(rdd: RDD[Int]): Unit = {

    val mappedRdd = rdd.map( (a) => 2 * a + 12)
    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench01 is " + result)

  }

  def bench01ve(rdd: VectorizedRDD[Int]): Unit = {
    import scala.reflect.runtime.universe._

    val expr = reify( (a: Int) => 2 * a + 12)
    val mappedRdd = rdd.vemap( expr )  // TODO: find a way around "reify" (macro?)

    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench01 is " + result)

  }


  def bench02(): Unit = {

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

  def setup(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("RDDBench")

    new SparkContext(conf)
  }

  def finishUp(): Unit = {
    sc.stop()
  }




}
