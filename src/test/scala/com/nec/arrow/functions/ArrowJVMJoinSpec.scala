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

import com.nec.arrow.ArrowVectorBuilders.{withDirectFloat8Vector, withDirectIntVector}
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMJoinSpec extends AnyFreeSpec {
  "JVM sum works" in {
    val firstColumn: Seq[Double] =
      Seq(10.0, 20.0, 30.0, 40.0, 50.0)
    val secondColumn: Seq[Double] =
      Seq(100.0, 200.0, 300.0, 400.0, 500.0)
    val firstKey: Seq[Int] =
      Seq(1, 2, 3, 4, 5)
    val secondKey: Seq[Int] =
      Seq(5, 200, 800, 3, 1)

    withDirectFloat8Vector(firstColumn) { firstColumnVec =>
      withDirectFloat8Vector(secondColumn) { secondColumnCec =>
        withDirectIntVector(firstKey) { firstKeyVec =>
          withDirectIntVector(secondKey) { secondKeyVec =>
            assert(
              Join.joinJVM(firstColumnVec, secondColumnCec, firstKeyVec, secondKeyVec) ==
                Seq((10.0, 500.0), (30.0, 400.0), (50.0, 100.0))
            )
          }
        }
      }
    }
  }
}
