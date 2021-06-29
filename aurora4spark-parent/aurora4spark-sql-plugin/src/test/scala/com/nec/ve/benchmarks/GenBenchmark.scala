package com.nec.ve.benchmarks

abstract class GenBenchmark[T <: GenBenchmarkState] {
  final val queries: Seq[String] = {
    Seq(
      "SELECT SUM(a) FROM nums_parquet",
      "SELECT SUM(a) FROM nums_csv"
    )
  }

  val sessionState: Class[T]
}
