package com.nec.spark.planning

import org.apache.spark.SparkContext

import scala.language.implicitConversions
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import scala.concurrent.duration.NANOSECONDS

trait PlanMetrics {
  def BATCH = "batch"
  def PLAN = "plan"
  def VE = "VE"

  def sparkContext: SparkContext
  def longMetric(name: String): SQLMetric

  def invocationMetrics(metricPrefix: String) = Map(
    s"${metricPrefix}Exec" -> SQLMetrics.createTimingMetric(sparkContext, s"${metricPrefix} execution time"),
    s"${metricPrefix}Invocations" -> SQLMetrics.createMetric(sparkContext, s"${metricPrefix} invocation count")
  )

  def incrementInvocations(metricPrefix: String): Unit = {
    longMetric(s"${metricPrefix}Invocations").add(1)
  }

  def withInvocationMetrics[T](metricPrefix: String)(f: => T): T = {
    val execMetric = longMetric(s"${metricPrefix}Exec")
    val invokeMetric = longMetric(s"${metricPrefix}Invocations")

    invokeMetric.set(1)
    val beforeExec = System.nanoTime()
    val res = f
    execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)

    res
  }

}
