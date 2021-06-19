package com.nec.ve

import com.nec.spark._
import com.nec.ve.VEWordCountSpec.WordCountQuery
import org.apache.spark.sql.execution.VeBasedBenchmark

object VeBenchmarkApp extends VeBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    implicit val willRun: BenchmarkFilter = name =>
      mainArgs.isEmpty ||
        mainArgs.exists(arg => name.contains(arg))

    Aurora4SparkExecutorPlugin.closeAutomatically = false
    try {
      runBenchmark("ve vs jvm benchmark") {
        val N = 2
        veBenchmark("agg w/o group", N) {
          spark.sql("SELECT SUM(_1 + _2) FROM nums")
        }
        veBenchmark("word count", N) {
          spark.sql(WordCountQuery)
        }
        veBenchmark("single column sum", N) {
          spark.sql("SELECT SUM(a) FROM nums")
        }
        veBenchmark("pairwise", N) {
          spark.sql("SELECT a + b FROM nums_parquet")
        }
      }
    } finally {
      Aurora4SparkExecutorPlugin.closeProcAndCtx()
    }
  }
}
