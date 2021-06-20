package com.nec.ve

import com.nec.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.debug.DebugQuery
import org.apache.spark.sql.internal.SQLConf
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class VEJMHBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test1VERunWithArrow(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._arrowEnabled = true
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(s"VE result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test1VERunNoArrow(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._arrowEnabled = false
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(s"VE result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRun(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(s"JVM result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(query.queryExecution.executedPlan)
    println(s"JVM result = ${query.collect().toList}")
  }
}
