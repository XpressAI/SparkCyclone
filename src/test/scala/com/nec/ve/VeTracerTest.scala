package com.nec.ve

import com.nec.cmake.TracerTest
import com.nec.native.{NativeCompiler, NativeEvaluator}
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.bytedeco.veoffload.global.veo
import org.scalatest.BeforeAndAfterAll

final class VeTracerTest extends TracerTest with BeforeAndAfterAll {

  override def includeUdp: Boolean = true

  private val (_, compiler) = NativeCompiler.fromTemporaryDirectory(VeCompilerConfig.testConfig)

  private var initialized = false
  private lazy val proc = {
    initialized = true
    veo.veo_proc_create(0)
  }

  override lazy val evaluator: NativeEvaluator = new VectorEngineNativeEvaluator(proc, compiler)

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      veo.veo_proc_destroy(proc)
    }
  }

}
