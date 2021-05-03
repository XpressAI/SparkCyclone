package com.nec.spark.agile

import com.nec.spark.agile.BigDecimalSummer.ScalaSummer
import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

final class SummingSparkPlanMultipleColumnsSpec extends AnyFreeSpec {
  "We can sum a general RDD" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    try {
      val inputData: Seq[ColumnWithNumbers] = Seq((0, Seq(1, 2, 3, 4, 5)), (1, Seq(6, 7, 8, 9, 10)))
      val result =
        SummingSparkPlanMultipleColumns
          .summingRdd(sparkSession.sparkContext.parallelize(inputData), ScalaSummer)
          .collect()
          .toSeq

      assert(result == Seq(List(15.0, 40.0)))
    } finally sparkSession.close()
  }
}
