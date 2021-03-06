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
package io.sparkcyclone.native.compiler

import org.apache.spark.SparkConf
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class VeCompilerConfigSpec extends AnyWordSpec {
  val config = VeCompilerConfig.fromSparkConf(
    new SparkConf().setAll(
      Seq(
        "spark.cyclone.ncc.debug" -> "true",
        "spark.cyclone.ncc.o" -> "3",
        "spark.cyclone.ncc.openmp" -> "false",
        "spark.cyclone.ncc.extra-argument.0" -> "-X",
        "spark.cyclone.ncc.extra-argument.1" -> "-Y"
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
