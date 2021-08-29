package com.nec.spark

import com.nec.native.NativeCompiler
import com.nec.native.NativeCompiler.CachingNativeCompiler

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext

import java.nio.file.Files
import com.nec.ve.VeKernelCompiler
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions

object Aurora4SparkDriverPlugin {
  // For assumption testing purposes only for now
  private[spark] var launched: Boolean = false
}

class Aurora4SparkDriverPlugin extends LazyLogging {

  private[spark] var nativeCompiler: NativeCompiler = _
  def receive(message: Any): AnyRef = {
    message match {
      case RequestCompiledLibraryForCode(code) =>
        logger.debug(s"Received request for compiled code: '${code}'")
        val localLocation = nativeCompiler.forCode(code)
        logger.info(s"Local compiled location = '${localLocation}'")
        RequestCompiledLibraryResponse(Files.readAllBytes(localLocation).toVector)
    }
  }

  def init(sc: SparkContext): java.util.Map[String, String] = {
    nativeCompiler = CachingNativeCompiler(NativeCompiler.fromConfig(sc.getConf))
    logger.info(s"Aurora4Spark DriverPlugin is launched. Will use compiler: ${nativeCompiler}")
    logger.info(s"Will use native compiler: ${nativeCompiler}")
    Aurora4SparkDriverPlugin.launched = true
    import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector._
    val sparkSessionExtensions =
      SparkSession.getActiveSession
        .orElse(SparkSession.getDefaultSession)
        .get
        .readPrivate
        .get[SparkSessionExtensions]("extensions")
    List(classOf[LocalVeoExtension]).foreach { extensionClass =>
      val instance = extensionClass.newInstance()
      instance.apply(sparkSessionExtensions)
    }

    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    val testArgs: Map[String, String] = Map(
      "ve_so_name" -> VeKernelCompiler
        .compile_c(
          buildDir = tmpBuildDir,
          config = VeKernelCompiler.VeCompilerConfig.fromSparkConf( /*pluginContext.conf()*/ null)
        )
        .toAbsolutePath
        .toString
    )
    testArgs.asJava
  }

  def shutdown(): Unit = {
    Aurora4SparkDriverPlugin.launched = false
  }
}
