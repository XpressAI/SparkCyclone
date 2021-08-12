package com.nec.cmake
import com.nec.arrow.functions.CsvParse
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.NativeCsvExec.transformInputStream
import com.nec.spark.planning.NativeCsvExec.transformRawTextFile
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.Text
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.io.ByteArrayInputStream

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
