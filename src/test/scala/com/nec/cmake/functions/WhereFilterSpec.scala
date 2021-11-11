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
package com.nec.cmake.functions
import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.CArrowNativeInterface
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.WhereFilterSpec.FilterSource
import com.nec.spark.agile.CppResource
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

object WhereFilterSpec {
  val FilterSource: String = CppResource("cpp/filterer.cc").readString

  def runOn(
    nativeInterface: ArrowNativeInterface
  )(inputVector: Float8Vector, outputVector: Float8Vector): Unit = {
    nativeInterface.callFunctionWrapped(
      "filter_doubles_over_15",
      List(NativeArgument.input(inputVector), NativeArgument.output(outputVector))
    )
  }

}

final class WhereFilterSpec extends AnyFreeSpec {
  "Through Arrow, it works" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", FilterSource)
        .mkString("\n\n")
    )

    withDirectFloat8Vector(input) { vector =>
      WithTestAllocator { alloc =>
        val outVector = new Float8Vector("value", alloc)
        try {
          WhereFilterSpec.runOn(new CArrowNativeInterface(cLib.toString))(vector, outVector)
          val outData = (0 until outVector.getValueCount).map(idx => outVector.get(idx)).toList
          assert(outData == List[Double](1, 2, 14))
        } finally outVector.close()
      }
    }
  }

}
