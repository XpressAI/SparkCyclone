package com.nec.native

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.DeferredArrowInterfaceNumeric
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.VeArrowNativeInterfaceNumeric.VeArrowNativeInterfaceNumericLazyLib
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler.CNativeCompiler
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.typesafe.scalalogging.LazyLogging

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
    ctx: Aurora.veo_thr_ctxt,
    nativeCompiler: NativeCompiler
  ) extends NativeEvaluator
    with LazyLogging {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
      val localLib = nativeCompiler.forCode(code).toString
      logger.debug(s"For evaluation, will use local lib '$localLib'")
      new VeArrowNativeInterfaceNumericLazyLib(proc, ctx, localLib)
    }
  }

  case object ExecutorPluginManagedEvaluator extends NativeEvaluator with LazyLogging {
    def forCode(code: String): ArrowNativeInterfaceNumeric = {
      // defer because we need the executors to initialize first
      logger.debug(s"For evaluation, will refer to the Executor Plugin")
      DeferredArrowInterfaceNumeric(() =>
        new VeArrowNativeInterfaceNumericLazyLib(
          Aurora4SparkExecutorPlugin._veo_proc,
          Aurora4SparkExecutorPlugin._veo_ctx,
          Aurora4SparkExecutorPlugin.libraryStorage.getLocalLibraryPath(code).toString
        )
      )
    }
  }

}
