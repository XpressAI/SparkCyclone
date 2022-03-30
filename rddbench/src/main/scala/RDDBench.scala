import com.nec.ve.VeRDD
import com.nec.ve.VeRDD._
import com.nec.ve.VeRDD.VeRichSparkContext
import com.nec.util.DateTimeOps._
import scala.collection.mutable.{Map => MMap}
import scala.reflect.runtime.universe.reify
import java.time.Instant
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object RDDBench {
  val timings = MMap.empty[String, Double]

  def basicBenchmark(sc: SparkContext): Unit = {
    println("Basic RDD Benchmark")

    println("Making RDD[Long]")
    val numbers = (1L to (1 * 1000000))

    val start1 = System.nanoTime()
    val rdd = sc.parallelize(numbers).repartition(8).cache()
    val result1 = benchmark("01 - CPU") {
      rdd.map((a: Long) => 2 * a + 12)
        .filter((a: Long) => a % 128 == 0)
        .groupBy((a: Long) => a % 2)
        .flatMap { case (k: Long, values: Iterable[Long]) => values }
        .reduce((a: Long, b: Long) => a + b)
        // .groupBy((a: Long) => a % 2)
        // .flatMap { case (k: Long, values: Iterable[Long]) => values }
    }
    val rddCount = rdd.count()
    val end1 = System.nanoTime()

    println("Making VeRDD[Long]")
    val start2 = System.nanoTime()
    val verdd = sc.veParallelize(numbers)
    val result2 = benchmark("01 - VE ") {
      verdd
        .vemap(reify { (a: Long) => 2 * a + 12 } )
        .vefilter(reify { (a: Long) => a % 128 == 0 })
        .vegroupBy(reify { (a: Long) => a % 2 })
        .toRDD
        .flatMap((a: (Long, Iterable[Long])) => a._2)
        .reduce((a: Long, b: Long) => a + b)
        // .toRDD      //.vereduce(reify { (a: Long, b: Long) => a + b })
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
    val rdd = sc.parallelize(1L to 1000000)
      .map(offset => Instant.parse("2022-02-28T08:18:50.957303Z").plusSeconds(offset - 5000))
      .repartition(8)
      .cache()
    val result1 = benchmark("02 - CPU") {
      rdd
        .filter((a: Instant) => a.compareTo(Instant.parse("2022-02-28T08:18:50.957303Z")) < 0)
        .map((a: Instant) => Instant.parse("2022-02-28T08:18:50.957303Z").compareTo(a) + 1L)
        .reduce((a: Long, b: Long) => a + b)
        .toLong
    }
    val rddCount = rdd.count
    val end1 = System.nanoTime

    println("Making VeRDD[Instant]")
    val start2 = System.nanoTime()
    val verdd = rdd.toVeRDD
    val result2 = benchmark("02 - VE ") {
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
    for ((title, duration) <- timings) {
      println(title + "\t" + duration + "seconds")
    }
    println("============================")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDBench")
    val sc = new SparkContext(conf)

    basicBenchmark(sc)
    timestampsBenchmark(sc)

    dumpResult()
    sc.stop
  }
}
