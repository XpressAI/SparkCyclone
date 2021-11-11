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
package com.nec.spark

import com.nec.spark.VeKernelCompilerConfigSpec.stringValue
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.SparkConf

object VeKernelCompilerConfigSpec {

  private def compilerConfig: VeCompilerConfig = VeCompilerConfig.fromSparkConf(
    new SparkConf().setAll(
      List(
        "spark.com.nec.spark.ncc.debug" -> "true",
        "spark.com.nec.spark.ncc.o" -> "3",
        "spark.com.nec.spark.ncc.openmp" -> "false",
        "spark.com.nec.spark.ncc.extra-argument.0" -> "-X",
        "spark.com.nec.spark.ncc.extra-argument.1" -> "-Y"
      )
    )
  )

  private def stringValue = compilerConfig.compilerArguments.toString

}
final class VeKernelCompilerConfigSpec extends AnyFreeSpec {
  "it captures DEBUG option" in {
    assert(stringValue.contains("DEBUG=1"))
  }
  "it captures Optimization override" in {
    assert(stringValue.contains("-O3"))
  }
  "It captures disabling OpenMP" in {
    assert(!stringValue.contains("openmp"))
  }
  "It can include extra arguments" in {
    assert(stringValue.contains("-X, -Y"))
  }
}
