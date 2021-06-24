package com.nec.ve

import com.nec.spark._
import org.openjdk.jmh.annotations._
import com.nec.debugging.Debugging._

@State(Scope.Benchmark)
class VEJMHBenchmarkCSV {
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test1VERunWithArrow(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._arrowEnabled = true
    import sparkVeSession.sparkSession.implicits._
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv").as[Double]
    println(query.queryExecution.executedPlan)
    query.debugSqlAndShow(name = "csv-sum-ve-bench")
    println(s"VE result = ${query.collect().toList}")
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRunNoWholestageCodegen(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(s"JVM result = ${query.collect().toList}")
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRunWithWholestageCodegen(sparkWholestageSession: SparkWholestageSession): Unit = {
    val query = sparkWholestageSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(query.queryExecution.executedPlan)
    query.debugSqlAndShow(name = "csv-sum-wholestage-bench")
    println(s"JVM result = ${query.collect().toList}")
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(query.queryExecution.executedPlan)
    println(s"JVM result = ${query.collect().toList}")
  }
}
