package com.nec.ve

import com.nec.spark._
import org.openjdk.jmh.annotations._
import com.nec.debugging.Debugging._

@State(Scope.Benchmark)
class VEJMHBenchmarkParquet {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test1VERunWithArrow(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._arrowEnabled = true
    import sparkVeSession.sparkSession.implicits._
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums").as[Double]
    println(query.queryExecution.executedPlan)
    query.debugSqlAndShow(name = "parquet-sum-ve-bench")
    println(s"VE result = ${query.collect().toList}")
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test1VERunNoArrow(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._arrowEnabled = false
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(s"VE result = ${query.collect().toList}")
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRunNoWholestageCodegen(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(s"JVM result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRunWithWholestageCodegen(sparkWholestageSession: SparkWholestageSession): Unit = {
    val query = sparkWholestageSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(query.queryExecution.executedPlan)
    query.debugSqlAndShow(name = "parquet-sum-jvm-bench")
    println(s"JVM result = ${query.collect().toList}")
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(query.queryExecution.executedPlan)
    println(s"JVM result = ${query.collect().toList}")
  }
}
