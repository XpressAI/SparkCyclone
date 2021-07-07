package com.nec.spark

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.nio.file.Files
import com.nec.ve.VeKernelCompiler

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
    val nativeCsvEnabled =
      sc.getConf.get("spark.com.nec.enable-native-csv", "true").equalsIgnoreCase("true")
    val allExtensions =
      List(classOf[LocalVeoExtension]) ++ (if (nativeCsvEnabled) List(classOf[NativeCsvExtension])
                                           else Nil)
    pluginContext
      .conf()
      .set("spark.sql.extensions", allExtensions.map(_.getCanonicalName).mkString(","))
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    val testArgs: Map[String, String] = Map(
      "ve_so_name" -> VeKernelCompiler
        .compile_c(
          buildDir = tmpBuildDir,
          config = VeKernelCompiler.VeCompilerConfig.fromSparkConf(pluginContext.conf())
        )
        .toAbsolutePath
        .toString
    )
    testArgs.asJava
  }

  override def shutdown(): Unit = {
    Aurora4SparkDriver.launched = false
  }
}
