package com.nec.ve.benchmarks

import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession

object RapidsCodegenBenchmark extends GenBenchmark[RapidsVeSessionState] {

  override val sessionState: Class[RapidsVeSessionState] = classOf[RapidsVeSessionState]
}

@State(Scope.Benchmark)
class RapidsVeSessionState extends GenBenchmarkState {
  var sparkSession: SparkSession = null

  @Setup
  def prepare(): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false

    this.sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
      .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
      .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")
      .config(key = "spark.ui.enabled", value = false)
      .getOrCreate()

    readData(sparkSession)
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
  }
}
