import com.nec.ve.VeRDD
import com.nec.ve.VeRDD.{VeRichSparkContext, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.time.Instant
import scala.collection.mutable.{Map => MMap}
import scala.reflect.runtime.universe.reify

object RDDBench {
  val timings: MMap[String, Double] = MMap.empty

  def checkRuns(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    println("Making VeRDD[Long]")
    val numbers = (1L to (1 * 1000))

    val start2 = System.nanoTime()
    val verdd = sc.veParallelize(numbers)

    benchmark("checkRuns - tuple input - VE") {
      sc.parallelize(numbers.zip(numbers))
        .toVeRDD
        .vemap(reify{(a: (Long, Long)) => a._1 + a._2})
        .vereduce(reify {(a: Long, b: Long) => a + b})
    }

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

  def basicBenchmark(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    println("Basic RDD Benchmark")

    println("Making RDD[Long]")
    val numbers = (1L to (1000 * 1000000))

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
    rdd.unpersist(true)
  }

  def timestampsBenchmark(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
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
    rdd.unpersist(true)
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

  def generateData(spark: SparkSession): String = {
    val sc = spark.sparkContext
    val data = sc.veParallelize(1L to (100L * 1000000L))
    val rows = data.map((a: Long) => (a, (a * 2).toInt, (a * 3).toFloat, (a * 4), (a * 5).toDouble))
    val reversed = rows.sortBy((tup) => tup._1, ascending = false)
    val path = s"sampledata-${Instant.now().toEpochMilli}"
    reversed.saveAsObjectFile(path)
    path
  }

  def fileBenchmark(spark: SparkSession, path: String): Unit = {
    val sc = spark.sparkContext
    println("File RDD Benchmark CPU")

    val rdd = sc.objectFile[(Long, Int, Float, Long, Double)](path, 8).cache()

    val result1 = benchmark("Stress Test on HDFS Data - CPU") {
      rdd
        .filter((a: (Long, Int, Float, Long, Double)) => a._1 % 2 == 0)
        .map((a: (Long, Int, Float, Long, Double)) => (a._1, a._2 * a._4, a._3 / 2.0f, a._5 * a._3))
        .reduce((tupA: (Long, Long, Float, Double), tupB: (Long, Long, Float, Double)) => (tupA._1 + tupB._1, tupA._2 + tupB._2, tupA._3 + tupB._3, tupA._4 + tupB._4))
    }

    println("File RDD Benchmark VE")
    val verdd = rdd.toVeRDD
    val result2 = benchmark("Stress Test on HDFS Data - VE") {
      verdd
        .filter((a: (Long, Int, Float, Long, Double)) => a._1 % 2 == 0)
        .map((a: (Long, Int, Float, Long, Double)) => (a._1, a._2 * a._4, a._3 / 2.0f, a._5 * a._3))
        .reduce((tupA: (Long, Long, Float, Double), tupB: (Long, Long, Float, Double)) => (tupA._1 + tupB._1, tupA._2 + tupB._2, tupA._3 + tupB._3, tupA._4 + tupB._4))
    }
    println(s"Values ${result1}, ${result2}.  Match: ${result1 == result2}")
    rdd.unpersist(true)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDBench")
      .getOrCreate()

    val sc = spark.sparkContext

    checkRuns(spark)
    basicBenchmark(spark)
    timestampsBenchmark(spark)
    val path = generateData(spark)
    fileBenchmark(spark, path)

    println(s"Cleaning up ${path}")
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(path), true) } catch { case _ : Throwable => { } }

    dumpResult()
    sc.stop
  }
}
