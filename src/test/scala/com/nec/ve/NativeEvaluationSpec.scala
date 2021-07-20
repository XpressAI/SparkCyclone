package com.nec.ve
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

object NativeEvaluationSpec {}

final class NativeEvaluationSpec extends AnyFreeSpec with BeforeAndAfterAll {

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

  "When we try to load from a random non-existing directory, it would fail" in {
    an[Exception] shouldBe thrownBy {
      new VectorEngineNativeEvaluator(proc, NativeCompiler.PreCompiled("/abced"))
        .forCode("abcd")
        .callFunction("test", Nil, Nil)
    }
  }

  "When we try to load from a directory which has had compilation, we should not have any exception" in {
    val (path, compiler) = NativeCompiler.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val someCode = "extern \"C\" long test() { return 0; }"
    new VectorEngineNativeEvaluator(proc, compiler)
      .forCode(someCode)
      .callFunction("test", Nil, Nil)
    new VectorEngineNativeEvaluator(proc, NativeCompiler.PreCompiled(path.toString))
      .forCode(someCode)
      .callFunction("test", Nil, Nil)
  }

  "When we try to load from a directory which has had compilation, but not for this .so, it should fail" in {
    val (path, compiler) = NativeCompiler.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val someCode = "extern \"C\" long test() { return 0; }"
    new VectorEngineNativeEvaluator(proc, compiler)
      .forCode(someCode)
      .callFunction("test", Nil, Nil)
    an[Exception] shouldBe thrownBy {
      new VectorEngineNativeEvaluator(proc, NativeCompiler.PreCompiled(path.toString))
        .forCode(someCode)
        .callFunction("test2", Nil, Nil)
    }
  }
}
