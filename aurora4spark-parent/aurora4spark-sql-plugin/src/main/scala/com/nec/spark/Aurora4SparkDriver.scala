package com.nec.spark

import com.nec.VeDirectApp.compile_c

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.nio.file.Files
import com.nec.VeCompiler

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
    pluginContext.conf().set("spark.sql.extensions", classOf[LocalVeoExtension].getCanonicalName)
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    // Just to test that the arguments are passed correctly as a starting point.
    // We gonna change this to actual params as soon as we know them.
    val testArgs: Map[String, String] = Map(
      "ve_so_name" -> compile_c(
        buildDir = tmpBuildDir,
        config = VeCompiler.VeCompilerConfig.fromSparkConf(pluginContext.conf())
      ).toAbsolutePath.toString
    )
    testArgs.asJava
  }
}
