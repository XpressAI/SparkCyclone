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

  val PartitioningFunction: CFunction = CFunction(
    hasSets = true,
    inputs = List(VeScalarType.VeNullableDouble.makeCVector("input")),
    outputs = List(VeScalarType.VeNullableDouble.makeCVector("o_p")),
    body = CodeLines
      .from(
        "int SETS_TO_DO = 5;",
        "int MAX_SET_ID = SETS_TO_DO - 1;",
        "for ( int s = 0; s < SETS_TO_DO; s++ ) {",
        CodeLines
          .from(
            "sets = s + 1;",
            "std::vector<double> nums();",
            "for ( int i = 0; i < input[0]->count; i++ ) {",
            CodeLines
              .from(
                "double v = input[0]->data[i];",
                "if ( (v >= s * 100 && v < (s + 1) * 100) || s == MAX_SET_ID ) {",
                CodeLines.from("nums.push_back(v);").indented,
                "}"
              )
              .indented,
            "}",
            GroupByOutline
              .scalarVectorFromStdVector(VeScalarType.VeNullableDouble, "o_p[s]", "nums")
          )
          .indented,
        "}"
      )
  )
}
