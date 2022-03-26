import com.nec.ve.VeRDD
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
    val billion = 1000000000L
    val numbers = (1L to (2L * billion))
    val rdd = sc.parallelize(numbers).repartition(16).cache()

    benchmark("01 - CPU",  () => bench01cpu(rdd))

    println("Making VeRDD")

    val verdd = new VeRDD(rdd)

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
    for ( (title, duration) <- timings) {
      println(title + "\t" + duration + "seconds")
    }
    println("============================")
  }


  def bench01cpu(rdd: RDD[Long]): Long = {
    val mappedRdd = rdd.map( (a) => 2 * a + 12)
    val filtered = mappedRdd.filter((a: Long) => a % 2 == 0)
    val result = filtered.reduce( (a,b) => a + b)

    println("result of bench01 is " + result)
    result
  }

  def bench01ve(rdd: VeRDD[Long]): Long = {
    val mappedRdd = rdd.map((a: Long) => 2 * a + 12)
    val filtered = mappedRdd.filter((a: Long) => a % 2 == 0)
    val result = filtered.reduce((a: Long, b: Long) => a + b)

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
