package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.StringProducer.{FilteringProducer, ImpCopyStringProducer}
import com.nec.spark.agile.groupby.GroupByOutline

object MergerFunction {

  def merge(types: List[VeType]): CFunction2 = CFunction2(
    arguments = List(
      List(CFunctionArgument.Raw("int batches"), CFunctionArgument.Raw("int rows")),
      types.zipWithIndex.map { case (veType, idx) =>
        CFunctionArgument.PointerPointer(veType.makeCVector(s"input_${idx}_g"))
      },
      types.zipWithIndex.map { case (veType, idx) =>
        CFunctionArgument.PointerPointer(veType.makeCVector(s"output_${idx}_g"))
      }
    ).flatten,
    body = CodeLines.from(types.zipWithIndex.map { case (veT, idx) =>
      val varName = s"output_${idx}"
      CodeLines.from(
        CodeLines.debugValue(s"$idx"),
        GroupByOutline.declare(veT.makeCVector(varName)),
        s"${varName} = ${varName}_g[0];",
        veT match {
          case VeString =>
            val fp_0 = FilteringProducer(varName, null)
            CodeLines.from(
              GroupByOutline.initializeStringVector(varName),
              fp_0.setup,
              CodeLines.forLoop("b", "batches")({
                val fp = FilteringProducer(varName, ImpCopyStringProducer(s"input_${idx}_g[b]"))
                CodeLines.from(fp.forEach)
              }),
              fp_0.complete,
              CodeLines.forLoop("i", "rows")(fp_0.validityForEach("i"))
            )
          case veScalarType: VeScalarType =>
            CodeLines.from(
              GroupByOutline.initializeScalarVector(veScalarType, varName, "rows"),
              "int o = 0;",
              CodeLines.forLoop("b", "batches") {
                CodeLines.from(CodeLines.forLoop("i", s"input_${idx}_g[b]->count") {
                  CodeLines.from(
                    s"$varName->data[o] = input_${idx}_g[b]->data[i];",
                    s"set_validity($varName->data, o, check_valid(input_${idx}_g[b]->validityBuffer, i));",
                    "o++;"
                  )
                })
              }
            )
        }
      )
    })
  )

}
