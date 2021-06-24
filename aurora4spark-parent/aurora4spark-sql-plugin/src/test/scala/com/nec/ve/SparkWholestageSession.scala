package com.nec.ve

import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import org.openjdk.jmh.annotations.{Scope, Setup, State, TearDown}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DoubleType

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
      .config(key = "spark.sql.codegen.fallback", value = false)
      .getOrCreate()

    import sparkSession.sqlContext.implicits._

    sparkSession.sqlContext.read
      .format("parquet")
      .load("/home/william/large-sample-parquet-10_9/")
      .createOrReplaceTempView("nums")

    val schema = StructType(
      Array(
        StructField("a", DoubleType),
        StructField("b", DoubleType),
        StructField("c", DoubleType)
      )
    )
    sparkSession.sqlContext.read
      .format("csv")
      .option("header", true)
      .schema(schema)
      .load("/home/dominik/large-sample-csv-10_9/")
      .as[(Double, Double, Double)]
      .createOrReplaceTempView("nums_csv")
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
  }
}
