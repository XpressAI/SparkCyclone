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

import io.sparkcyclone.native.compiler.CppResource.CppResources
import java.nio.file.Files
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class CppResourcesSpec extends AnyWordSpec {
  val LowerBound = CppResource(
    "frovedis/core/lower_bound.hpp",
    "/com/nec/cyclone/cpp/frovedis/core/lower_bound.hpp"
  )

  "CppResources" should {
    "correctly list lower_bound.hpp" in {
      CppResources.All.all should contain (LowerBound)
    }

    "be able to copy resources" in {
      val tmpdir = Files.createTempDirectory("test")
      val file = tmpdir.resolve(LowerBound.name)
      LowerBound.copyTo(tmpdir)
      Files.exists(file) should be (true)
    }
  }
}
