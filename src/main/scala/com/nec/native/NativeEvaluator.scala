package com.nec.native

import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.DeferredArrowInterface
import com.nec.arrow.CArrowNativeInterface
import com.nec.arrow.VeArrowNativeInterface.VeArrowNativeInterfaceLazyLib
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler.{CNativeCompiler, CNativeCompilerDebug}
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.typesafe.scalalogging.LazyLogging

trait NativeEvaluator extends Serializable {
  def forCode(code: String): ArrowNativeInterface
}

object NativeEvaluator {

  /** Selected when running in CMake mode */
  object CNativeEvaluator extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterface = {
      new CArrowNativeInterface(CNativeCompiler.forCode(code).toAbsolutePath.toString)
    }
  }
  final case class CNativeEvaluator(debug: Boolean) extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterface = {
      new CArrowNativeInterface(
        (if (debug) CNativeCompilerDebug else CNativeCompiler).forCode(code).toAbsolutePath.toString
      )
    }
  }

  final class VectorEngineNativeEvaluator(
    proc: Aurora.veo_proc_handle,
    nativeCompiler: NativeCompiler
  ) extends NativeEvaluator
    with LazyLogging {
    override def forCode(code: String): ArrowNativeInterface = {
      val localLib = nativeCompiler.forCode(code).toString
      logger.debug(s"For evaluation, will use local lib '$localLib'")
      new VeArrowNativeInterfaceLazyLib(proc, localLib)
    }
  }

  case object ExecutorPluginManagedEvaluator extends NativeEvaluator with LazyLogging {
    def forCode(code: String): ArrowNativeInterface = {
      // defer because we need the executors to initialize first
      logger.debug(s"For evaluation, will refer to the Executor Plugin")
      DeferredArrowInterface(() =>
        new VeArrowNativeInterfaceLazyLib(
          Aurora4SparkExecutorPlugin._veo_proc,
          Aurora4SparkExecutorPlugin.libraryStorage.getLocalLibraryPath(code).toString
        )
      )
    }
  }

}
