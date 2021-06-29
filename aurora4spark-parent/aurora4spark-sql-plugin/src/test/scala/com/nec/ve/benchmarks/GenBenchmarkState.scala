package com.nec.ve.benchmarks

import org.apache.spark.sql.SparkSession

trait GenBenchmarkState {

  def readData(sparkSession: SparkSession): Unit = {
    sparkSession.sqlContext.read
      .format("parquet")
      .load("/home/william/large-sample-parquet-10_9/")
      .createOrReplaceTempView("nums_parquet")

    sparkSession.sqlContext.read
      .format("csv")
      .option("header", true)
      .load("/home/dominik/large-sample-csv-10_9/")
      .createOrReplaceTempView("nums_csv")
  }
}
