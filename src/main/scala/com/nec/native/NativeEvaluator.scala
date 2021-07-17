package com.nec.native

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.DeferredArrowInterfaceNumeric
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.VeArrowNativeInterfaceNumeric.VeArrowNativeInterfaceNumericLazyLib
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler.CNativeCompiler
import com.nec.spark.Aurora4SparkExecutorPlugin

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
  ) extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterfaceNumeric = {
      new VeArrowNativeInterfaceNumericLazyLib(proc, ctx, nativeCompiler.forCode(code).toString)
    }
  }

  case object ExecutorPluginManagedEvaluator extends NativeEvaluator {
    def forCode(code: String): ArrowNativeInterfaceNumeric = {
      // defer because we need the executors to initialize first
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
