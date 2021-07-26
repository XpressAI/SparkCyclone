package com.nec.spark

import org.apache.spark.internal.Logging

class AuroraSqlPlugin extends Logging {
  def driverPlugin(): Aurora4SparkDriverPlugin = new Aurora4SparkDriverPlugin()

  def executorPlugin(): Aurora4SparkExecutorPlugin = new Aurora4SparkExecutorPlugin()
}
