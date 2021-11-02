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
import com.nec.cmake.NativeReaderSpec.unixSocketToNativeToArrow
import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.bytedeco.veoffload.global.veo
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

final class UnixSocketToVeToArrowSpec extends AnyFreeSpec with BeforeAndAfterAll {

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
  "We can read-write with a unix socket" ignore {
    val (path, compiler) = NativeCompiler.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val inputList = List("ABC", "DEF", "GHQEWE")
    if (!scala.util.Properties.isWin) {
      val expectedString = inputList.mkString
      assert(
        unixSocketToNativeToArrow(
          new VectorEngineNativeEvaluator(proc, compiler),
          inputList
        ) == expectedString
      )
    }
  }
}
