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

import com.nec.cmake.TracerTest
import com.nec.native.{NativeCompiler, NativeEvaluator}
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.ve.VeKernelCompiler.{ProfileTarget, VeCompilerConfig}
import org.bytedeco.veoffload.global.veo
import org.scalatest.BeforeAndAfterAll

final class VeTracerTest extends TracerTest with BeforeAndAfterAll {

  override def includeUdp: Boolean = true

  private val profileTarget = ProfileTarget(host = "127.0.0.1", port = 45705)
  private val config = VeCompilerConfig.testConfig.copy(maybeProfileTarget = Some(profileTarget))
  private val (_, compiler) = NativeCompiler.fromTemporaryDirectory(config)

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
