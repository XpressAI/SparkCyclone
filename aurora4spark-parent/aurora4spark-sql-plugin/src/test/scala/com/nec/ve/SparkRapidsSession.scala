package com.nec.ve

import com.nec.spark.Aurora4SparkExecutorPlugin
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession

@State(Scope.Benchmark)
class SparkRapidsSession {
  var _sparkSession: SparkSession = null
  lazy val sparkSession: SparkSession = _sparkSession

  @Setup
  def prepare(): Unit = {

    this._sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
      .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
      .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")

      .config(key = "spark.ui.enabled", value = false)
      .getOrCreate()

    sparkSession.sqlContext.read
      .format("parquet")
      .load("/home/william/large-sample-parquet-10_9/")
      .createOrReplaceTempView("nums")

    sparkSession.sqlContext.read
      .format("csv")
      .option("header", true)
      .load("/home/dominik/large-sample-csv-10_9/")
      .createOrReplaceTempView("nums_csv")
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
  }
}
