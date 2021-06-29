package com.nec.ve.benchmarks

import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession

object VeCodegenBenchmark extends GenBenchmark[SparkWholestageSessionState] {

  override val sessionState: Class[SparkWholestageSessionState] = classOf[SparkWholestageSessionState]
}

@State(Scope.Benchmark)
class SparkVeSessionState extends GenBenchmarkState {
  var _sparkSession: SparkSession = null
  lazy val sparkSession: SparkSession = _sparkSession

  @Setup
  def prepare(): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false

    this._sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
      .config(key = "spark.ui.enabled", value = false)
      .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
      .config(key = org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, value = false)
      .getOrCreate()

    readData(sparkSession)
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
  }
}
