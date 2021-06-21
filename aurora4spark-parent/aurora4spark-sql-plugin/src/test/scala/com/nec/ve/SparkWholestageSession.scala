package com.nec.ve

import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession

@State(Scope.Benchmark)
class SparkWholestageSession {
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
      .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
      .getOrCreate()

    sparkSession.sqlContext.read
      .format("parquet")
      .load("/home/william/large-sample-parquet-10_9/")
      .createOrReplaceTempView("nums")
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
  }
}
