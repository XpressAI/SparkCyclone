package com.nec.arrow
import com.nec.aurora.Aurora
import com.nec.spark.Aurora4SparkExecutorPlugin

final case class ExecutorDeferredVeArrowNativeInterfaceNumeric(driverLibPath: String)
  extends ArrowNativeInterfaceNumeric {
  override def callFunctionGen(
    name: String,
    inputArguments: List[Option[ArrowNativeInterfaceNumeric.SupportedVectorWrapper]],
    outputArguments: List[Option[ArrowNativeInterfaceNumeric.SupportedVectorWrapper]]
  ): Unit = {
    // use for debugging purposes
    // println(s"Loading: ${Aurora4SparkExecutorPlugin._veo_proc}; ${libPath}; ${Thread.currentThread}")
    val libPath = Aurora4SparkExecutorPlugin.libraryStorage.getLibrary(driverLibPath)
    val lib = Aurora.veo_load_library(Aurora4SparkExecutorPlugin._veo_proc, libPath)
    require(lib != 0, s"Expected lib != 0, got ${lib}")
    println(s"Evaluating $name on ${libPath}...")
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
