package com.nec.spark.agile

import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final class AveragingSparkPlanSpec extends AnyFreeSpec {
  "We can average a general RDD" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      val result =
        AveragingSparkPlan
          .averagingRdd(Seq[Double](100, 200).toDS().rdd, AveragingSparkPlan.averageLocal)
          .collect()
          .head
      assert(result == 150d)
    } finally sparkSession.close()
  }
}
