package com.nec.spark

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.internal.Logging

class AuroraSqlPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new Aurora4SparkDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = new Aurora4SparkExecutorPlugin()
}
