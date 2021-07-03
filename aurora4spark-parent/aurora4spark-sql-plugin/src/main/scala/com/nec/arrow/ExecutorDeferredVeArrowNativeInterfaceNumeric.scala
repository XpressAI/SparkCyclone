package com.nec.arrow
import com.nec.aurora.Aurora
import com.nec.spark.Aurora4SparkExecutorPlugin

final case class ExecutorDeferredVeArrowNativeInterfaceNumeric(libPath: String)
  extends ArrowNativeInterfaceNumeric {
  override def callFunctionGen(
    name: String,
    inputArguments: List[Option[ArrowNativeInterfaceNumeric.SupportedVectorWrapper]],
    outputArguments: List[Option[ArrowNativeInterfaceNumeric.SupportedVectorWrapper]]
  ): Unit = {
    println(s"Loading: ${Aurora4SparkExecutorPlugin._veo_proc}; ${libPath}; ${Thread.currentThread}")
    val lib = try Aurora.veo_load_library(Aurora4SparkExecutorPlugin._veo_proc, libPath)
    require(lib != 0, s"Expected lib != 0, got ${lib}")
    try {
      new VeArrowNativeInterfaceNumeric(
        Aurora4SparkExecutorPlugin._veo_proc,
        Aurora4SparkExecutorPlugin._veo_ctx,
        lib
      ).callFunctionGen(name, inputArguments, outputArguments)
    } finally {
      Aurora.veo_unload_library(Aurora4SparkExecutorPlugin._veo_proc, lib)
    }
  }
}
