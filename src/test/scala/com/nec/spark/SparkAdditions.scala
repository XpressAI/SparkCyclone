package com.nec.spark

import com.nec.spark.planning.VERewriteStrategy
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkAdditions {

  protected def logLevel: Level = Level.ERROR

  Logger.getRootLogger.setLevel(logLevel)

  protected def withSpark[T](configure: SparkConf => SparkConf)(f: SparkContext => T): T = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.ui.enabled", "false")
    val sparkContext = new SparkContext(configure(conf))

    try {
      f(sparkContext)
    } finally sparkContext.stop()
  }

  protected def withSparkSession[T](configure: SparkConf => SparkConf)(f: SparkSession => T): T = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.ui.enabled", "false")
    val sparkSession = SparkSession.builder().config(configure(conf)).getOrCreate()
    try f(sparkSession)
    finally sparkSession.stop()
  }

  protected def withSparkSession2[T](
    configure: SparkSession.Builder => SparkSession.Builder
  )(f: SparkSession => T): T = {
    VERewriteStrategy.failFast = true
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.ui.enabled", "false")
    val sparkSession = configure(SparkSession.builder().config(conf)).getOrCreate()
    try f(sparkSession)
    finally sparkSession.stop()
  }

}
