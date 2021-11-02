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
package com.nec.spark.agile

import com.nec.spark.agile.CppResource.CppResources
import com.nec.spark.agile.ListCppResourcesSpec.LowerBound
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files

object ListCppResourcesSpec {
  val LowerBound = CppResource("cpp/frovedis/core/lower_bound.hpp")
}

final class ListCppResourcesSpec extends AnyFreeSpec {

  "It lists lower_bound.hpp" in {
    com.eed3si9n.expecty.Expecty.assert(CppResources.All.all.contains(LowerBound))
  }

  "A resource can be copied" in {
    val tempDir = Files.createTempDirectory("tst")
    val expectedFile = tempDir.resolve(LowerBound.name)
    LowerBound.copyTo(tempDir)
    assert(Files.exists(expectedFile))
  }

}
