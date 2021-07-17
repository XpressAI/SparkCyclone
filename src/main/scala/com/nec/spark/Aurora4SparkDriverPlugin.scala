package com.nec.spark

import com.nec.native.NativeCompiler
import com.nec.native.NativeCompiler.CachingNativeCompiler

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.api.plugin.PluginContext

import java.nio.file.Files
import com.nec.ve.VeKernelCompiler
import com.typesafe.scalalogging.LazyLogging
import okio.ByteString

object Aurora4SparkDriverPlugin {
  // For assumption testing purposes only for now
  private[spark] var launched: Boolean = false
}

class Aurora4SparkDriverPlugin extends DriverPlugin with LazyLogging {

  private[spark] var nativeCompiler: NativeCompiler = _
  override def receive(message: Any): AnyRef = {
    message match {
      case RequestCompiledLibraryForCode(code) =>
        logger.debug(s"Received request for compiled code: '${code}'")
        val localLocation = nativeCompiler.forCode(code)
        logger.info(s"Local compiled location = '${localLocation}'")
        RequestCompiledLibraryResponse(ByteString.of(Files.readAllBytes(localLocation): _*))
      case other => super.receive(message)
    }
  }

  override def init(
    sc: SparkContext,
    pluginContext: PluginContext
  ): java.util.Map[String, String] = {
    nativeCompiler = CachingNativeCompiler(NativeCompiler.fromConfig(sc.getConf))
    logger.info(s"Aurora4Spark DriverPlugin is launched. Will use compiler: ${nativeCompiler}")
    logger.info(s"Will use native compiler: ${nativeCompiler}")
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
