package com.nec.ve

import com.nec.spark._
import com.nec.ve.VEWordCountSpec.WordCountQuery

import org.apache.spark.sql.execution.VeBasedBenchmark

object VeBenchmarkOffheapVsArrowApp extends VeBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false
    try {
      runBenchmark("ve vs jvm benchmark") {
        val N = 2
        offHeapVsArrowBenchmark("single column sum", N) {
          spark.sql("SELECT SUM(_1) FROM nums")
        }
      }
    } finally {
      Aurora4SparkExecutorPlugin.closeProcAndCtx()
    }
  }
}
