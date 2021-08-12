package com.nec.native

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.DeferredArrowInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.ExecutorInterfaceWithDirectLibrary
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.VeArrowNativeInterfaceNumeric.VeArrowNativeInterfaceNumericLazyLib
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler.CNativeCompiler
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.Files

trait NativeEvaluator extends Serializable {
  def forCode(code: String): ArrowNativeInterfaceNumeric
}

object NativeEvaluator {

  /** Selected when running in CMake mode */
  object CNativeEvaluator extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
      new CArrowNativeInterfaceNumeric(CNativeCompiler.forCode(code).toAbsolutePath.toString)
    }
  }

  final class VectorEngineNativeEvaluator(
    proc: Aurora.veo_proc_handle,
    nativeCompiler: NativeCompiler
  ) extends NativeEvaluator
    with LazyLogging {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
      val localLib = nativeCompiler.forCode(code).toString
      logger.debug(s"For evaluation, will use local lib '$localLib'")
      new VeArrowNativeInterfaceNumericLazyLib(proc, localLib)
    }
  }

  final class BroadcastEvaluator(nativeCompiler: NativeCompiler) extends NativeEvaluator with LazyLogging {
    override def forCode(
      code: String
    ): ArrowNativeInterfaceNumeric = {
      val localLib = nativeCompiler.forCode(code)
      val libValue = Files.readAllBytes(localLib)
      ExecutorInterfaceWithDirectLibrary(code, libValue.toVector)
    }
  }

}
