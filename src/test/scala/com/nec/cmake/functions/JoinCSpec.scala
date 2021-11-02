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

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.CArrowNativeInterface
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec
import com.nec.arrow.functions.Join._
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.JoinCSpec.JoinerSource
import com.nec.spark.agile.CppResource

object JoinCSpec {
  val JoinerSource: String = CppResource("cpp/joiner.cc").readString
}

final class JoinCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val soPath = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", JoinerSource)
        .mkString("\n\n")
    )

    WithTestAllocator { alloc =>
      val outVector = new Float8Vector("value", alloc)
      val firstColumn: Seq[Double] = Seq(5, 1, 2, 34, 6)
      val secondColumn: Seq[Double] = Seq(100, 15, 92, 331, 49)
      val firstColumnKeys: Seq[Int] = Seq(1, 2, 3, 4, 5)
      val secondColumnKeys: Seq[Int] = Seq(4, 2, 5, 200, 800)
      try ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
        ArrowVectorBuilders.withDirectFloat8Vector(secondColumn) { secondColumnVec =>
          ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
            ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
              runOn(new CArrowNativeInterface(soPath.toString))(
                firstColumnVec,
                secondColumnVec,
                firstKeysVec,
                secondKeysVec,
                outVector
              )
              val res = (0 until outVector.getValueCount)
                .map(i => outVector.get(i))
                .toList
                .splitAt(outVector.getValueCount / 2)
              val joinResult = res._1.zip(res._2)
              assert(
                joinResult == joinJVM(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec)
              )
            }
          }
        }
      } finally outVector.close()
    }
  }

}
