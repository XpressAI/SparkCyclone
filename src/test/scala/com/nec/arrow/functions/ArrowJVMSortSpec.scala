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
package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.{withArrowFloat8Vector, withDirectFloat8Vector}
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMSortSpec extends AnyFreeSpec {
  "JVM sum works" in {
    val inputData: Seq[Double] =
      Seq(100.0, 10.0, 20.0, 8.0, 900.0, 1000.0, 1.0)

    withDirectFloat8Vector(inputData) { vcv =>
      assert(Sort.sortJVM(vcv) == Seq(1.0, 8.0, 10.0, 20.0, 100.0, 900.0, 1000.0))
    }
  }
}
