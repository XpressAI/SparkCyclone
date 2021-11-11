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

import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.CsvParse
import com.nec.cmake.functions.ParseCSVSpec.verifyOn
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_thr_ctxt
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

final class ParseVECSVSpec extends AnyFreeSpec {
  "We can do a run of CSV" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectories(veBuildPath)
    val soPath = VeKernelCompiler(
      compilationPrefix = "csv",
      buildDir = veBuildPath,
      config = VeKernelCompiler.VeCompilerConfig.testConfig
        .copy(doDebug = false)
    ).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode)
        .mkString("\n\n")
    )

    val proc = veo.veo_proc_create(0)
    try {
      val ctx: veo_thr_ctxt = veo.veo_context_open(proc)
      try {
        WithTestAllocator { alloc =>
          val outVector = new Float8Vector("value", alloc)
          val lib: Long = veo.veo_load_library(proc, soPath.toString)
          verifyOn(new VeArrowNativeInterface(proc, lib))
        }
      } finally veo.veo_context_close(ctx)
    } finally veo.veo_proc_destroy(proc)
  }
}
