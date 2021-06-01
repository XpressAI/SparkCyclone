package com.nec.ve
import org.apache.spark.sql.execution.VeBasedBenchmark

object VeBenchmarkApp extends VeBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("sum") {
      val N = 500L << 3
      veBenchmark("agg w/o group", N) {
        spark.sql("SELECT SUM(_1 + _2) FROM nums")
      }
    }
  }
}
