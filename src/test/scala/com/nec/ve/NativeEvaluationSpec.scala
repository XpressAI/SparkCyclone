/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.ve
import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.bytedeco.veoffload.global.veo
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

object NativeEvaluationSpec {}

final class NativeEvaluationSpec extends AnyFreeSpec with BeforeAndAfterAll {

  private var initialized = false
  private lazy val proc = {
    initialized = true
    veo.veo_proc_create(0)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      veo.veo_proc_destroy(proc)
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
