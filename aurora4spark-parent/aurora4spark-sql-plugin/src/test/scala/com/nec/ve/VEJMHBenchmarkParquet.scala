package com.nec.ve

import com.nec.spark._
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class VEJMHBenchmarkParquet {
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  def test1VERunWithArrow(sparkVeSession: SparkVeSession): Unit = {
//    LocalVeoExtension._arrowEnabled = true
//    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
//    println(s"VE result = ${query.collect().toList}")
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  def test1VERunNoArrow(sparkVeSession: SparkVeSession): Unit = {
//    LocalVeoExtension._arrowEnabled = false
//    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
//    println(s"VE result = ${query.collect().toList}")
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  def test2JVMRunNoWholestageCodegen(sparkVeSession: SparkVeSession): Unit = {
//    LocalVeoExtension._enabled = false
//    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums")
//    println(s"JVM result = ${query.collect().toList}")
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  def test2JVMRunWithWholestageCodegen(sparkWholestageSession: SparkWholestageSession): Unit = {
//    val query = sparkWholestageSession.sparkSession.sql("SELECT SUM(a) FROM nums")
//    println(s"JVM result = ${query.collect().toList}")
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
//    LocalVeoExtension._enabled = false
//    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums")
//    println(query.queryExecution.executedPlan)
//    println(s"Rapids result = ${query.collect().toList}")
//  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testWholestageSum(rapidsSession: SparkVeSession): Unit = {
    LocalVeoExtension._enabled = true
    LocalVeoExtension._useCodegenPlans = true
    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums")
    println(query.queryExecution.executedPlan)
    println(s"VE wholestage codegen result = ${query.collect().toList}")
  }
}
