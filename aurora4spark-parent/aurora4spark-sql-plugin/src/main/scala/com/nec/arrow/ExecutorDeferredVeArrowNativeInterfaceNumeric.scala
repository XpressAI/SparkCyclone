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
    val lib = Aurora.veo_load_library(Aurora4SparkExecutorPlugin._veo_proc, libPath)
    new VeArrowNativeInterfaceNumeric(
      Aurora4SparkExecutorPlugin._veo_proc,
      Aurora4SparkExecutorPlugin._veo_ctx,
      lib
    ).callFunctionGen(name, inputArguments, outputArguments)
  }
}
