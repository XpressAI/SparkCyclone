package com.nec.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.freespec.AnyFreeSpec

final class SqlPluginTest extends AnyFreeSpec {
  "It is not launched if not specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    val sparkContext = new SparkContext(conf)

    try {
      assert(
        !Aurora4SparkDriver.launched,
        "Expect the driver to have not been launched"
      )
    } finally sparkContext.stop()
  }

  "It is launched if specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set(
      "spark.plugins",
      classOf[SqlPlugin].getName
    )
    val sparkContext = new SparkContext(conf)
    try {
      assert(
        Aurora4SparkDriver.launched,
        "Expect the driver to have been launched"
      )
    } finally sparkContext.stop()
  }

  "We can run a Spark-SQL job" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
      assert(result == List(3))
    } finally sparkSession.close()
  }
}
