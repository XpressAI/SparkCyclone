package org.apache.spark.sql.execution
import com.nec.spark.AuroraSqlPlugin
import com.nec.spark.LocalVeoExtension
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.Dataset

import java.util.UUID
import com.nec.spark.SampleTestData

trait VeBasedBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(key = SQLConf.SHUFFLE_PARTITIONS.key, value = 1)
      .config(key = SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, value = 1)
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
      .config(key = UI_ENABLED.key, value = false)
      .getOrCreate()
  }

  trait BenchmarkFilter {
    def willRun(name: String): Boolean
  }


  final def veBenchmark[T](name: String, cardinality: Long)(
    ds: => Dataset[T]
  )(implicit benchmarkFilter: BenchmarkFilter): Unit = {
    if (!benchmarkFilter.willRun(name)) return;
    val benchmark = new Benchmark(name, cardinality, output = output)

    import spark.implicits._
    Seq
      .fill[(Double, Double)](200000)(
        (scala.util.Random.nextDouble(), scala.util.Random.nextDouble())
      )
      .toDS()
      .createOrReplaceTempView("nums")

    spark.read
      .format("parquet")
      .load(SampleTestData.SampleTwoColumnParquet.toString)
      .createOrReplaceTempView("nums_parquet")

    spark.read
      .format("parquet")
      .load(SampleTestData.SampleTwoColumnParquet.toString)
      .createOrReplaceTempView("nums_parquet")

    List
      .fill[String](10000)(UUID.randomUUID.toString)
      .toDS()
      .withColumnRenamed("value", "word")
      .createOrReplaceTempView("words")

    LocalVeoExtension._enabled = true
    println("VE plan:")
    ds.explain()

    println("non-VE plan:")
    LocalVeoExtension._enabled = false
    ds.explain()

    benchmark.addCase(s"$name on NEC SX-Aurora TSUBASA", numIters = 5) { _ =>
      LocalVeoExtension._enabled = true

      withSQLConf(("spark.sql.columnVector.offheap.enabled", "true")) {
        ds.noop()
      }
    }

    benchmark.addCase(s"$name on Spark JVM", numIters = 5) { _ =>
      LocalVeoExtension._enabled = false
      ds.noop()
    }

    benchmark.run()
  }

  final def offHeapVsArrowBenchmark[T](name: String, cardinality: Long)(ds: => Dataset[T]): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    import spark.implicits._
    Seq
      .fill[(Double, Double)](1000000)(
        (scala.util.Random.nextDouble(), scala.util.Random.nextDouble())
      )
      .toDS()
      .createOrReplaceTempView("nums")

    List
      .fill[String](10000)(UUID.randomUUID.toString)
      .toDS()
      .withColumnRenamed("value", "word")
      .createOrReplaceTempView("words")

    LocalVeoExtension._arrowEnabled = true
    println("Arrow based plan:")
    ds.explain()

    println("OffHeap based plan:")
    LocalVeoExtension._arrowEnabled = false
    ds.explain()

    benchmark.addCase(s"$name Arrow based computation", numIters = 5) { _ =>
      LocalVeoExtension._arrowEnabled = true

      withSQLConf(
        ("spark.sql.columnVector.offheap.enabled", "true")
      ) {
        ds.noop()
      }
    }

    benchmark.addCase(s"$name pure OffHeap based computation", numIters = 5) { _ =>
      LocalVeoExtension._enabled = false
      ds.noop()
    }

    benchmark.run()
  }
}
