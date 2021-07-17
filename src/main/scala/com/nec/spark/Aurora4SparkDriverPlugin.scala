package com.nec.spark

import com.nec.native.NativeCompiler

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.nio.file.Files
import com.nec.ve.VeKernelCompiler
import okio.ByteString

object Aurora4SparkDriverPlugin {
  // For assumption testing purposes only for now
  private[spark] var launched: Boolean = false
}

class Aurora4SparkDriverPlugin extends DriverPlugin with Logging {

  private[spark] var nativeCompiler: NativeCompiler = _
  override def receive(message: Any): AnyRef = {
    message match {
      case RequestCompiledLibraryForCode(code) =>
        RequestCompiledLibraryResponse(
          ByteString.of(Files.readAllBytes(nativeCompiler.forCode(code)): _*)
        )
      case other => super.receive(message)
    }
  }

  override def init(
    sc: SparkContext,
    pluginContext: PluginContext
  ): java.util.Map[String, String] = {
    nativeCompiler = NativeCompiler.fromConfig(sc.getConf)
    logInfo("Aurora4Spark DriverPlugin is launched.")
    Aurora4SparkDriverPlugin.launched = true
    val allExtensions = List(classOf[LocalVeoExtension], classOf[NativeCsvExtension])
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
    Aurora4SparkDriverPlugin.launched = false
  }
}
