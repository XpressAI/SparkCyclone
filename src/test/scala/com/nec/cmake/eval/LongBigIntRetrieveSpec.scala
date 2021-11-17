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
package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.util.RichVectors.RichBigIntVector
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.apache.arrow.vector.BigIntVector
import org.scalatest.freespec.AnyFreeSpec

final class LongBigIntRetrieveSpec extends AnyFreeSpec {
  "It works" in {
    WithTestAllocator { implicit allocator =>
      val outVector_0 = new BigIntVector("output_0", allocator)

      val cLib = CMakeBuilder.buildC(
        List(
          TransferDefinitionsSourceCode,
          "\n\n",
          CodeLines
            .from(
              """ extern "C" long x(nullable_bigint_vector *v) { """,
              "v->count = 1;",
              "v->data = (int64_t *)malloc(v->count * sizeof(int64_t));",
              "v->validityBuffer = (uint64_t *) malloc(ceil(v->count / 64.0) * sizeof(uint64_t)); ",
              "v->data[0] = 123;",
              "set_validity(v->validityBuffer, 0, 1); ",
              "return 0;",
              """ }"""
            )
            .cCode
        )
          .mkString("\n\n")
      )

      val nativeInterface = new CArrowNativeInterface(cLib.toString)
      val res =
        try {
          nativeInterface.callFunctionWrapped("x", List(NativeArgument.output(outVector_0)))
          outVector_0.toList
        } finally { outVector_0.close() }

      assert(res == List[Long](123))
    }
  }

}
