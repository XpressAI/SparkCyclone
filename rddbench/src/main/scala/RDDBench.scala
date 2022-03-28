import com.nec.ve.VeRDD
import com.nec.ve.VeRDD.VeRichSparkContext
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object RDDBench {

  var sc: SparkContext = _
  var timings: Map[String, Double] = Map()

  def main(args: Array[String]): Unit = {
    println("RDD Benchmark")

    println("setup")
    sc = setup()

    println("Making numbers")
    val numbers = (1L to (1000 * 1000000))
    val rdd = sc.parallelize(numbers).repartition(8).cache()

    benchmark("01 - CPU", () => bench01cpu(rdd))

    println("Making VeRDD")


    //val verdd: VeRDD[Long] = rdd.toVeRDD
    val verdd = sc.veParallelize(numbers)

    println(s"rdd has ${rdd.count()}. verdd has ${verdd.count()} rows.")

    println("Starting Benchmark")

    benchmark("01 - VE ", () => bench01ve(verdd))

    dumpResult()
    //Thread.sleep(300 * 1000)
    finishUp()
  }

  var lastVal: Long = 0

  def benchmark(title: String, f: () => Long): Unit = {
    val ts_start = System.nanoTime()
    if (lastVal == 0) {
      lastVal = f()
    } else {
      val newVal = f()
      println(s"Values match: ${lastVal == newVal}")
    }
    val ts_end = System.nanoTime()
    val diff = (ts_end - ts_start) / 1000000000.0
    timings = timings + (title -> diff)
  }

  def dumpResult(): Unit = {
    println("======== RESULTS ===========")
    for ((title, duration) <- timings) {
      println(title + "\t" + duration + "seconds")
    }
    println("============================")
  }


  def bench01cpu(rdd: RDD[Long]): Long = {
    val result = rdd.map((a: Long) => 2 * a + 12)
      .filter((a: Long) => a % 128 == 0)
      .groupBy((a: Long) => a % 2)
      .flatMap { case (k: Long, values: Iterable[Long]) => values }
      .reduce((a: Long, b: Long) => a + b)

    println("result of bench01 is " + result)
    result
  }

  def bench01ve(rdd: VeRDD[Long]): Long = {
    val result = rdd
      .map((a: Long) => 2 * a + 12)
      .filter((a: Long) => a % 128 == 0)
      .groupBy((a: Long) => a % 2)
      .flatMap((a: (Long, Iterable[Long])) => a._2)
      .reduce((a: Long, b: Long) => a + b)

    println("result of bench01 is " + result)
    result
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
