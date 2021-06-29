package com.nec.ve.benchmarks

import com.nec.spark.Aurora4SparkExecutorPlugin
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession

object JvmNoWholestageCodegenBenchmark extends GenBenchmark[SparkJVMNoWholestageSessionState] {

  override val sessionState: Class[SparkJVMNoWholestageSessionState] = classOf[SparkJVMNoWholestageSessionState]
}

@State(Scope.Benchmark)
class SparkJVMNoWholestageSessionState extends GenBenchmarkState {
  var _sparkSession: SparkSession = null
  lazy val sparkSession: SparkSession = _sparkSession

  @Setup
  def prepare(): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false

    this._sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(key = "spark.ui.enabled", value = false)
      .config(key = org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, value = false)
      .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
      .getOrCreate()

    readData(_sparkSession)
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
  }

}