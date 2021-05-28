package com.nec.spark

import com.nec.VeDirectApp.compile_c
import com.nec.spark.Aurora4SparkDriver.ve_so_name

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

object Aurora4SparkDriver {
  private lazy val ve_so_name = compile_c()

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
    val testArgs: Map[String, String] = Map("ve_so_name" -> ve_so_name.toAbsolutePath.toString)
    testArgs.asJava
  }
}
