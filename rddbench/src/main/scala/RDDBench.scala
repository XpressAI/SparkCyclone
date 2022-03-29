import com.nec.ve.VeRDD
import com.nec.ve.VeRDD.VeRichSparkContext
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.universe.reify

object RDDBench {

  var sc: SparkContext = _
  var timings: Map[String, Double] = Map()

  def main(args: Array[String]): Unit = {
    println("RDD Benchmark")

    println("setup")
    sc = setup()

    println("Starting Benchmark")

    println("Making numbers")
    val numbers = (1L to (1 * 1000000))

    val start1 = System.nanoTime()
    val rdd = sc.parallelize(numbers).repartition(8).cache()
    benchmark("01 - CPU", () => bench01cpu(rdd))
    val rddCount = rdd.count()
    val end1 = System.nanoTime()


    println("Making VeRDD")
    val start2 = System.nanoTime()
    //val verdd: VeRDD[Long] = rdd.toVeRDD
    val verdd = sc.veParallelize(numbers)
    benchmark("01 - VE ", () => bench01ve(verdd))
    val verddCount = verdd.count()
    val end2 = System.nanoTime()

    dumpResult()
    println(s"vhrdd has ${rddCount} rows. (took ${(end1 - start1) / 1000000000} s total)")
    println(s"verdd has ${verddCount} rows. (took ${(end2 - start2) / 1000000000} s total)")

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

    //.groupBy((a: Long) => a % 2)
    //.flatMap { case (k: Long, values: Iterable[Long]) => values }

    println("result of bench01 is " + result)
    result
  }

  def bench01ve(rdd: VeRDD[Long]): Long = {
    val result = rdd
      .vemap(reify { (a: Long) => 2 * a + 12 } )
      .vefilter(reify { (a: Long) => a % 128 == 0 })
      .vegroupBy(reify { (a: Long) => a % 2 })
      .toRDD
      .flatMap((a: (Long, Iterable[Long])) => a._2)
      .reduce((a: Long, b: Long) => a + b)

    //.toRDD      //.vereduce(reify { (a: Long, b: Long) => a + b })

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
