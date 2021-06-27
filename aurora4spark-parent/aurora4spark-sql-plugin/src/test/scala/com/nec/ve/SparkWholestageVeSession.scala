package com.nec.ve

import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin, LocalVeoExtension}
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession

@State(Scope.Benchmark)
class SparkWholestageVeSession {
  var _sparkSession: SparkSession = null
  lazy val sparkSession: SparkSession = _sparkSession
  LocalVeoExtension._enabled = true
  LocalVeoExtension._useCodegenPlans = true

  @Setup
  def prepare(): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false

    this._sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
      .appName(this.getClass.getCanonicalName)
      .config(key = "spark.ui.enabled", value = false)
      .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
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
