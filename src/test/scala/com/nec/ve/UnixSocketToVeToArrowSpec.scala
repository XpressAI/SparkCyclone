package com.nec.ve
import com.nec.aurora.Aurora
import com.nec.cmake.NativeReaderSpec.unixSocketToNativeToArrow
import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

final class UnixSocketToVeToArrowSpec extends AnyFreeSpec with BeforeAndAfterAll {

  private var initialized = false
  private lazy val proc = {
    initialized = true
    Aurora.veo_proc_create(0)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      Aurora.veo_proc_destroy(proc)
    }
  }
  "We can read-write with a unix socket" in {
    val (path, compiler) = NativeCompiler.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val inputList = List("ABC", "DEF", "GHQEWE")
    if (!scala.util.Properties.isWin) {
      val expectedString = inputList.mkString
      assert(
        unixSocketToNativeToArrow(
          new VectorEngineNativeEvaluator(proc,  compiler),
          inputList
        ) == expectedString
      )
    }
  }
}
