package com.nec.ve

import com.nec.spark._
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class VEJMHBenchmarkCSV {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRunWithWholestageCodegen(sparkWholestageSession: SparkWholestageSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkWholestageSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(s"JVM result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRunNoWholestageCodegen(sparkVeSession: SparkVeSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(s"JVM result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
    LocalVeoExtension._enabled = false
    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(query.queryExecution.executedPlan)
    println(s"JVM result = ${query.collect().toList}")
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testWholestageSum(rapidsSession: SparkWholestageVeSession): Unit = {
    LocalVeoExtension._enabled = true
    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
    println(query.queryExecution.executedPlan)
    println(s"VE wholestage codegen result = ${query.collect().toList}")
  }
}
