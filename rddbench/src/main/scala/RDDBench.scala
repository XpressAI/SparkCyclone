import com.nec.ve.VeRDD
import com.nec.ve.VeRDD.{VeRichSparkContext, _}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.Instant
import scala.collection.mutable.{Map => MMap}
import scala.reflect.runtime.universe.reify

object RDDBench {
  val timings: MMap[String, Double] = MMap.empty

  def checkRuns(sc: SparkContext): Unit = {
    println("Making VeRDD[Long]")
    val numbers = (1L to (1 * 1000))

    val start2 = System.nanoTime()
    val verdd = sc.veParallelize(numbers)
    benchmark("checkRuns - join - VE") {
      val rdd1: VeRDD[(Long, Long)] = verdd.vemap(reify { (a: Long) => (3 * a, a * 2) }).vefilter(reify{(a: (Long, Long)) => a._1 % 16 == 0})
      val rdd2: VeRDD[(Long, Long)] = verdd.vemap(reify { (a: Long) => (3 * a, a * 7)}).vefilter(reify{(a: (Long, Long)) => a._1 % 8 == 0})
      rdd1.vejoin(rdd2).toRDD.collect().mkString(", ")
    }

    benchmark("checkRuns - reduce - VE ") {
      verdd
        .vemap(reify {(a: Long) => (3 * a, a * 2)})
        .vefilter(reify{(a: (Long, Long)) => a._1 % 16 == 0})
        .vesortBy(reify{(a: (Long, Long)) => a._1 % 4 })
        .vereduce(reify {(a: (Long, Long), b: (Long, Long)) => (a._1 + b._2, b._1 + a._2)})
    }
    benchmark("checkRuns - groupBy - VE ") {
      verdd
        .vemap(reify {(a: Long) => (3 * a, a * 2)})
        .vefilter(reify{(a: (Long, Long)) => a._1 % 16 == 0})
        .vesortBy(reify{(a: (Long, Long)) => a._1 % 4 })
        .vegroupBy(reify{(a: (Long, Long)) => a._2 % 7 })
        .toRDD
        .collect()
        .mkString(", ")
    }
    val verddCount = verdd.count()
    val end2 = System.nanoTime()

    println(s"verdd has ${verddCount} rows. (took ${(end2 - start2) / 1000000000} s total)")
  }

  def basicBenchmark(sc: SparkContext): Unit = {
    println("Basic RDD Benchmark")

    println("Making RDD[Long]")
    val numbers = (1L to (500 * 1000000))

    val start1 = System.nanoTime()
    val rdd = sc.parallelize(numbers).repartition(8).cache()
    val result1 = benchmark("Basic - CPU") {
      rdd
        .filter((a: Long) => a % 3 == 0 && a % 5 == 0 && a % 15 == 0)
        .map((a: Long) => ((a.toFloat / 2.0).toLong + 12) - (a % 15))
        .groupBy((a: Long) => a % 2)
        .flatMap((tup: (Long, Iterable[Long])) => tup._2.map(x => x * tup._1))
        .reduce((a: Long, b: Long) => a + b)
    }
    val rddCount = rdd.count()
    val end1 = System.nanoTime()

    println("Making VeRDD[Long]")
    val start2 = System.nanoTime()
    val verdd = sc.veParallelize(numbers)
    //val verdd = rdd.toVeRDD
    val result2 = benchmark("Basic - VE ") {
      verdd
        .filter((a: Long) => a % 3 == 0 && a % 5 == 0 && a % 15 == 0)
        .map((a: Long) => ((a.toFloat / 2.0).toLong + 12) - (a % 15))
        .groupBy((a: Long) => a % 2)
        .flatMap((tup: (Long, Iterable[Long])) => tup._2.map(x => x * tup._1))
        .reduce((a: Long, b: Long) => a + b)
    }
    val verddCount = verdd.count()
    val end2 = System.nanoTime()

    println(s"Values match: ${result1 == result2}")
    println(s"vhrdd has ${rddCount} rows. (took ${(end1 - start1) / 1000000000} s total)")
    println(s"verdd has ${verddCount} rows. (took ${(end2 - start2) / 1000000000} s total)")
  }

  def timestampsBenchmark(sc: SparkContext): Unit = {
    println("Timestamps RDD Benchmark")

    println("Making RDD[Instant]")
    val start1 = System.nanoTime
    val rdd = sc.parallelize(1L to 100 * 1000000)
      .map(offset => Instant.parse("2022-02-28T08:18:50.957303Z").plusSeconds(offset - 5000))
      .repartition(8)
      .cache()
    val result1 = benchmark("Instant - CPU") {
      rdd
        .filter((a: Instant) => a.compareTo(Instant.parse("2022-02-28T08:18:50.957303Z")) < 0)
        .map((a: Instant) => Instant.parse("2022-02-28T08:18:50.957303Z").compareTo(a) + 1L)
        .reduce((a: Long, b: Long) => a + b)
    }
    val rddCount = rdd.count
    val end1 = System.nanoTime

    println("Making VeRDD[Instant]")
    val start2 = System.nanoTime()
    val verdd = rdd.toVeRDD
    val result2 = benchmark("Instant - VE ") {
      verdd
        .filter((a: Instant) => a.compareTo(Instant.parse("2022-02-28T08:18:50.957303Z")) < 0)
        .map((a: Instant) => Instant.parse("2022-02-28T08:18:50.957303Z").compareTo(a) + 1L)
        .reduce((a: Long, b: Long) => a + b)
    }


    val verddCount = verdd.count()
    val end2 = System.nanoTime()

    println(s"Values match: ${result1 == result2}")
    println(s"vhrdd has ${rddCount} rows. (took ${(end1 - start1) / 1000000000} s total)")
    println(s"verdd has ${verddCount} rows. (took ${(end2 - start2) / 1000000000} s total)")
  }


  def benchmark[T](title: String)(func: => T): T = {
    val start = System.nanoTime
    val result = func
    val end = System.nanoTime()
    println(s"${title}: result = ${result}")
    // timings = timings + (title -> (end - start) / 1000000000.0)
    timings += (title -> (end - start) / 1000000000.0)
    result
  }

  def dumpResult(): Unit = {
    println("======== RESULTS ===========")
    for ((title, duration) <- timings.toList.sortBy(_._1)) {
      println(title + "\t" + duration + "seconds")
    }
    println("============================")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDBench")
    val sc = new SparkContext(conf)

    checkRuns(sc)
    basicBenchmark(sc)
    timestampsBenchmark(sc)

    dumpResult()
    sc.stop
  }
}
