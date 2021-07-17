package com.nec.native

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.ExecutorDeferredVeArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.cmake.CMakeBuilder
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

trait NativeEvaluator extends Serializable {
  def forCode(code: String): ArrowNativeInterfaceNumeric
  protected def combinedCode(code: String): String =
    List(TransferDefinitions.TransferDefinitionsSourceCode, code).mkString("\n\n")
}

object NativeEvaluator {
  def fromConfig(sparkConf: SparkConf): NativeEvaluator = {
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

  def fromTemporaryDirectory(compilerConfig: VeCompilerConfig): (Path, NativeEvaluator) = {
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    (tmpBuildDir, OnDemandCompilation(tmpBuildDir.toAbsolutePath.toString, compilerConfig))
  }

  final case class OnDemandCompilation(buildDir: String, veCompilerConfig: VeCompilerConfig)
    extends NativeEvaluator
    with LazyLogging {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
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
      ExecutorDeferredVeArrowNativeInterfaceNumeric(soName.toString)
    }
  }

  final case class PreCompiled(sourceDir: String) extends NativeEvaluator with LazyLogging {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
      val cc = combinedCode(code)
      val sourcePath =
        Paths.get(sourceDir).resolve(s"_spark_${cc.hashCode}").toAbsolutePath.toString
      logger.debug(s"Will be loading source from path: $sourcePath")
      ExecutorDeferredVeArrowNativeInterfaceNumeric(sourcePath)
    }
  }

  /** Selected when running in CMake mode */
  object CNativeEvaluator extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
      val cLib = CMakeBuilder.buildC(
        List(TransferDefinitions.TransferDefinitionsSourceCode, code)
          .mkString("\n\n")
      )
      new CArrowNativeInterfaceNumeric(cLib.toAbsolutePath.toString)
    }
  }

}
