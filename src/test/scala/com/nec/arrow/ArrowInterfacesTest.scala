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
package com.nec.arrow
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.util.RichVectors._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec
import org.bytedeco.javacpp.DoublePointer

final class ArrowInterfacesTest extends AnyFreeSpec {

  List[List[Double]](
    List(5),
    List(5, -5),
    List(5, -5, 10),
    List(5, -5, 10, 11),
    List(5, -5, 10, 11, -91),
    List(5, -5, 10, 11, -91, 5, -5, 10, 11, -91)
  ).foreach { list =>
    s"We can get a vector of size ${list.size} out without facing issues" in {
      val alloc = new RootAllocator(Integer.MAX_VALUE)
      try {
        val vector = new Float8Vector("value", alloc)
        try {
          val ndv = new non_null_double_vector
          ndv.count = list.size
          val bp = new DoublePointer(list.toArray:_*)

          ndv.data = bp.address()
          non_null_double_vector_to_float8Vector(ndv, vector)
          assert(vector.toList == list)
        } finally vector.close()
      } finally alloc.close()
    }
  }

}
