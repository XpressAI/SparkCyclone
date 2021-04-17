package com.nec.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

object Aurora4SparkDriver {

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
}

class Aurora4SparkDriver extends DriverPlugin with Logging {
  override def init(
      sc: SparkContext,
      pluginContext: PluginContext
  ): java.util.Map[String, String] = {
    logInfo("Aurura4Spark DriverPlugin is launched.")
    Aurora4SparkDriver.launched = true
    Map.empty[String, String].asJava
  }
}
