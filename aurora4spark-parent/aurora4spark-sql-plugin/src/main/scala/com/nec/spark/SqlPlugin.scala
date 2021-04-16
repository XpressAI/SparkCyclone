package com.nec.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging

class SqlPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = null
  override def executorPlugin(): ExecutorPlugin = null

}

class Aurora4SparkDriver extends DriverPlugin with Logging {
  override def init(sc: SparkContext, pluginContext: PluginContext): java.util.Map[String, String] =
    null
}

class Aurora4SparkExecutor extends ExecutorPlugin with Logging {
  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = null
}
