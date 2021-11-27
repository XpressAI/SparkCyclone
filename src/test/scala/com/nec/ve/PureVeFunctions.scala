package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, VeScalarType}
import com.nec.spark.agile.groupby.GroupByOutline

object PureVeFunctions {
  val DoublingFunction: CFunction = CFunction(
    inputs = List(VeScalarType.VeNullableDouble.makeCVector("input")),
    outputs = List(VeScalarType.VeNullableDouble.makeCVector("o_p")),
    body = CodeLines
      .from(
        CodeLines
          .from(
            "nullable_double_vector* o = (nullable_double_vector *)malloc(sizeof(nullable_double_vector));",
            "*o_p = o;",
            GroupByOutline
              .initializeScalarVector(VeScalarType.VeNullableDouble, "o", "input[0]->count"),
            "for ( int i = 0; i < input[0]->count; i++ ) {",
            CodeLines
              .from("o->data[i] = input[0]->data[i] * 2;", "set_validity(o->validityBuffer, i, 1);")
              .indented,
            "}"
          )
          .indented
      )
  )
}
