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
package com.nec.cmake
import com.nec.arrow.functions.CsvParse
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.NativeCsvExec.transformInputStream
import com.nec.spark.planning.NativeCsvExec.transformRawTextFile
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.Text
import org.scalatest.{BeforeAndAfter, Ignore}
import org.scalatest.freespec.AnyFreeSpec

import java.io.ByteArrayInputStream

/** Not used any more. Candidate for deletion */
@Ignore
final class NativeCsvExecSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with LazyLogging {

  "Transform a Text into a columnar batch" ignore {
    val colBatch = transformRawTextFile(
      3,
      3,
      CNativeEvaluator.forCode(CsvParse.CsvParseCode),
      "test-file",
      new Text("a,b,c\n1,2,3\n4.1,5,6\n")
    )(logger)

    val allItems = (0 until colBatch.numRows()).map { rowNum =>
      (
        colBatch.column(0).getDouble(rowNum),
        colBatch.column(1).getDouble(rowNum),
        colBatch.column(2).getDouble(rowNum)
      )
    }.toList

    assert(allItems == List((1d, 2d, 3d), (4.1000000000000005, 5d, 6d)))
  }

  "Transform this input stream thing into a columnar batch" in {
    if (!scala.util.Properties.isWin) {
      val is = new ByteArrayInputStream("a,b,c\n1,2,3\n4.1,5,6\n".getBytes())
      val colBatch =
        transformInputStream(
          3,
          3,
          CNativeEvaluator.forCode(CsvParse.CsvParseCode),
          "test-file",
          is
        )(logger)

      val allItems = (0 until colBatch.numRows()).map { rowNum =>
        (
          colBatch.column(0).getDouble(rowNum),
          colBatch.column(1).getDouble(rowNum),
          colBatch.column(2).getDouble(rowNum)
        )
      }.toList

      assert(allItems == List((1d, 2d, 3d), (4.1000000000000005, 5d, 6d)))
    }
  }

}
