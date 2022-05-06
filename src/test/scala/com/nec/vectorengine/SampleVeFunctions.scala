package com.nec.vectorengine

import com.nec.spark.agile.core._
import com.nec.spark.agile.core.CFunction2._
import com.nec.spark.agile.core.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline

object SampleVeFunctions {
  final val DoublingFunction: CFunction2 = {
    val body = CodeLines.from(
      "*output = nullable_double_vector::allocate();",
      "output[0]->resize(input[0]->count);",
      "",
      CodeLines.forLoop("i", "input[0]->count") {
        List(
          "output[0]->data[i] = input[0]->data[i] * 2;",
          "output[0]->set_validity(i, 1);"
        )
      }
    )

    CFunction2(
      "double_func",
      Seq(
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("input")),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("output"))
      ),
      body,
    )
  }
}
