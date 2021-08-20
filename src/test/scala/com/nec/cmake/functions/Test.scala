package com.nec.cmake.functions

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").getOrCreate()
    session.read.option("header", "true").csv("/Users/wosin/aurora4spark/src/test/resources/com/nec/spark/sampleMultiColumn-distributed.csv")
      .coalesce(1)
      .write
      .option("compression", "none")
      .parquet("outParquet")
  }

}
