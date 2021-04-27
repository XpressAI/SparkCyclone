package com.nec.spark.agile

import com.nec.spark.agile.BigDecimalSummer.ScalaSummer
import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final class SummingSparkPlanSpec extends AnyFreeSpec {
  "We can sum a general RDD" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      val result =
        SummingSparkPlan
          .summingRdd(Seq[Double](1, 2, 2).toDS().rdd, ScalaSummer)
          .collect()
          .head
      assert(result == BigDecimal(5))
    } finally sparkSession.close()
  }
}
