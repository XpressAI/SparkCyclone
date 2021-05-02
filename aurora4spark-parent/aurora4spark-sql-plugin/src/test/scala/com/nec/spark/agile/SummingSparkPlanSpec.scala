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
      implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Iterable[Double]]
      import sparkSession.implicits._
      val input: Seq[(Int, Seq[Double])] = Seq(
        (1, Seq(2D, 3D)),
        (3, Seq(4D, 5D))
      )
      val result =
        SummingSparkPlan
          .summingRdd(Seq[Double](1, 2, 2).toDS().rdd, ScalaSummer)
          .collect()
          .head
      assert(result == BigDecimal(5))
    } finally sparkSession.close()
  }
}
