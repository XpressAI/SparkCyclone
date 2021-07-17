package com.nec.native
import com.nec.arrow.TransferDefinitions
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.Paths
import com.nec.cmake.CMakeBuilder
import org.apache.spark.SparkConf

import java.nio.file.Files
import java.nio.file.Path
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.VeCompilerConfig

trait NativeCompiler extends Serializable {
  /** Location of the compiled kernel library */
  def forCode(code: String): Path
  protected def combinedCode(code: String): String =
    List(TransferDefinitions.TransferDefinitionsSourceCode, code).mkString("\n\n")
}

object NativeCompiler {
  def fromConfig(sparkConf: SparkConf): NativeCompiler = {
    val compilerConfig = VeKernelCompiler.VeCompilerConfig.fromSparkConf(sparkConf)
    sparkConf.getOption("spark.com.nec.spark.kernel.precompiled") match {
      case Some(directory) => PreCompiled(directory)
      case None =>
        sparkConf.getOption("spark.com.nec.spark.kernel.directory") match {
          case Some(directory) =>
            OnDemandCompilation(directory, compilerConfig)
          case None =>
            fromTemporaryDirectory(compilerConfig)._2
        }
    }
  }

  def fromTemporaryDirectory(compilerConfig: VeCompilerConfig): (Path, NativeCompiler) = {
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    (tmpBuildDir, OnDemandCompilation(tmpBuildDir.toAbsolutePath.toString, compilerConfig))
  }

  final case class OnDemandCompilation(buildDir: String, veCompilerConfig: VeCompilerConfig)
    extends NativeCompiler
    with LazyLogging {
    override def forCode(code: String): Path = {
      logger.debug(s"Compiling for the VE...: $code")
      val startTime = System.currentTimeMillis()
      val cc = combinedCode(code)
      val soName =
        VeKernelCompiler(
          compilationPrefix = s"_spark_${cc.hashCode}",
          Paths.get(buildDir),
          veCompilerConfig
        )
          .compile_c(cc)
      val endTime = System.currentTimeMillis() - startTime
      logger.debug(s"Compiled code in ${endTime}ms to path ${soName}.")
      soName
    }
  }

  final case class PreCompiled(sourceDir: String) extends NativeCompiler with LazyLogging {
    override def forCode(code: String): Path = {
      val cc = combinedCode(code)
      val sourcePath = Paths.get(sourceDir).resolve(s"_spark_${cc.hashCode}.so").toAbsolutePath
      logger.debug(s"Will be loading source from path: $sourcePath")
      sourcePath
    }
  }

  object CNativeCompiler extends NativeCompiler {
    override def forCode(code: String): Path = {
      CMakeBuilder.buildC(
        List(TransferDefinitions.TransferDefinitionsSourceCode, code)
          .mkString("\n\n")
      )
    }
  }

}
