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
package com.nec.native.compiler

import org.apache.spark.SparkConf
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class VeKernelCompilerConfigSpec extends AnyWordSpec {
  val config = VeCompilerConfig.fromSparkConf(
    new SparkConf().setAll(
      Seq(
        "spark.com.nec.spark.ncc.debug" -> "true",
        "spark.com.nec.spark.ncc.o" -> "3",
        "spark.com.nec.spark.ncc.openmp" -> "false",
        "spark.com.nec.spark.ncc.extra-argument.0" -> "-X",
        "spark.com.nec.spark.ncc.extra-argument.1" -> "-Y"
      )
    )
  )

  "VeCompilerConfig" should {
    "capture the DEBUG option" in {
      config.compilerFlags should contain ("DEBUG=1")
    }

    "capture the optimization override" in {
      config.compilerFlags should contain ("-O3")
    }

    "capture the disabling of OpenMP" in {
      config.compilerFlags should not contain ("-fopenmp")
    }

    "include the extra arguments" in {
      Seq("-X", "-Y").foreach { flag =>
        config.compilerFlags should contain (flag)
      }
    }
  }
}
