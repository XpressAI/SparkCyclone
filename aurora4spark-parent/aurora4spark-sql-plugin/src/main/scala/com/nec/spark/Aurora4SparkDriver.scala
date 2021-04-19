package com.nec.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters.mapAsJavaMapConverter

object Aurora4SparkDriver {

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
}

class Aurora4SparkDriver extends DriverPlugin with Logging {
  override def init(
    sc: SparkContext,
    pluginContext: PluginContext
  ): java.util.Map[String, String] = {
    logInfo("Aurora4Spark DriverPlugin is launched.")
    Aurora4SparkDriver.launched = true
    // Just to test that the arguments are passed correctly as a starting point.
    // We gonna change this to actual params as soon as we know them.
    val testArgs: Map[String, String] = Map("testArgument" -> "test")
    testArgs.asJava
  }
}
