package com.nec.ve

import com.nec.spark.agile.core._
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.agile.core.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline

object PureVeFunctions {
  val DoublingFunction: CFunction = CFunction(
    inputs = List(VeNullableDouble.makeCVector("input")),
    outputs = List(VeNullableDouble.makeCVector("o_p")),
    body = CodeLines
      .from(
        CodeLines
          .from(
            "nullable_double_vector* o = nullable_double_vector::allocate();",
            "*o_p = o;",
            GroupByOutline.initializeScalarVector(VeNullableDouble, "o", "input[0]->count"),
            "for ( int i = 0; i < input[0]->count; i++ ) {",
            CodeLines
              .from("o->data[i] = input[0]->data[i] * 2;", "o->set_validity(i, 1);")
              .indented,
            "}"
          )
          .indented
      )
  )

  val PartitioningFunction: CFunction = CFunction(
    hasSets = true,
    inputs = List(VeNullableDouble.makeCVector("input")),
    outputs = List(VeNullableDouble.makeCVector("o_p")),
    body = CodeLines
      .from(
        "int SETS_TO_DO = 5;",
        "int MAX_SET_ID = SETS_TO_DO - 1;",
        "*o_p = static_cast<nullable_double_vector*>(malloc(sizeof(nullptr) * SETS_TO_DO));",
        "for ( int s = 0; s < SETS_TO_DO; s++ ) {",
        CodeLines
          .from(
            "sets[0] = s + 1;",
            "std::vector<double> nums;",
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
              .scalarVectorFromStdVector(VeNullableDouble, "o_p[s]", "nums")
          )
          .indented,
        "}"
      )
  )
}
