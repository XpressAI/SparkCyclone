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
            CodeLines.debugValue("input[0]->count"),
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
        "std::vector<std::vector<double>> partitions = {};",
        "radix_partition(input[0]->data, input[0]->count, partitions);",
        "*o_p = (nullable_double_vector*)malloc(sizeof(void *) * partitions.size());",
        "*sets = partitions.size();",
        "for ( int s = 0; s < partitions.size(); s++) {",
          CodeLines.from(
            GroupByOutline.scalarVectorFromStdVector(
              VeScalarType.VeNullableDouble,
              "o_p[s]", "partitions[s]"
            )
          ).indented,
        "}"
      )
  )
}
