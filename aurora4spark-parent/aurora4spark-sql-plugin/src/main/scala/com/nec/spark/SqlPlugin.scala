package com.nec.spark

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.internal.Logging

class SqlPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new Aurora4SparkDriver()

  override def executorPlugin(): ExecutorPlugin = new Aurora4SparkExecutorPlugin()
}
