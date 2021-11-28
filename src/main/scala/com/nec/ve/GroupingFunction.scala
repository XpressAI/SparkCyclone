package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.VeType

object GroupingFunction {
  def groupData(
    groupingKeys: List[VeType],
    otherValues: List[VeType],
    totalBuckets: Int
  ): CFunction2 = {
    CFunction2(
      arguments = List(
        groupingKeys.zipWithIndex.map { case (veType, idx) =>
          CFunction2.CFunctionArgument.PointerPointer(veType.makeCVector(s"key_${idx}"))
        },
        otherValues.zipWithIndex.map { case (veType, idx) =>
          CFunction2.CFunctionArgument.PointerPointer(veType.makeCVector(s"value_${idx}"))
        },
        List(CFunctionArgument.Raw("int* sets")),
        groupingKeys.zipWithIndex.map { case (veType, idx) =>
          CFunction2.CFunctionArgument.PointerPointer(veType.makeCVector(s"key_output_${idx}"))
        },
        otherValues.zipWithIndex.map { case (veType, idx) =>
          CFunction2.CFunctionArgument.PointerPointer(veType.makeCVector(s"value_output_${idx}"))
        }
      ).flatten,
      body = CodeLines.from(
      )
    )
  }
}
