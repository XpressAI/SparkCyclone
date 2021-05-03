package com.nec.spark.agile

import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final class AveragingSparkPlanMultipleColumnsSpec extends AnyFreeSpec {
  "We can average a general RDD" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {

      val inputData: Seq[(Int, Seq[Double])] = Seq((0, Seq(10, 20, 30)), (1, Seq(100, 200, 300)))

      val result =
        AveragingSparkPlanMultipleColumns
          .averagingRdd(
            sparkSession.sparkContext.parallelize(inputData),
            AveragingSparkPlanMultipleColumns.averageLocalScala
          )
          .map(value => value.toList)
          .collect()
          .head

      assert(result == List(20.0, 200.0))
    } finally sparkSession.close()
  }
}
