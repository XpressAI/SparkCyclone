package com.nec.ve
import com.nec.native.NativeEvaluator
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

object NativeEvaluationSpec {}

final class NativeEvaluationSpec extends AnyFreeSpec {
  "When we try to load from a random non-existing directory, it would fail" in {
    an[Exception] shouldBe thrownBy {
      NativeEvaluator.PreCompiled("/abced").forCode("abcd").callFunction("test", Nil, Nil)
    }
  }

  "When we try to load from a directory which has had compilation, we should not have any exception" in {
    val (path, evaluator) = NativeEvaluator.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val someCode = "long test() { return 0; }"
    evaluator.forCode(someCode).callFunction("test", Nil, Nil)
    NativeEvaluator.PreCompiled(path.toString).forCode(someCode).callFunction("test", Nil, Nil)
  }

  "When we try to load from a directory which has had compilation, but not for this .so, it should fail" in {
    val (path, evaluator) = NativeEvaluator.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val someCode = "long test() { return 0; }"
    evaluator.forCode(someCode).callFunction("test", Nil, Nil)
    an[Exception] shouldBe thrownBy {
      NativeEvaluator.PreCompiled(path.toString).forCode(someCode).callFunction("test2", Nil, Nil)
    }
  }
}
