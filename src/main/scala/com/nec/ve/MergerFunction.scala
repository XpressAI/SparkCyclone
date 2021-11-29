package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.StringProducer.{FilteringProducer, FrovedisCopyStringProducer, ImpCopyStringProducer}
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
      val outputVarName = s"output_${idx}"
      CodeLines.from(
        CodeLines.debugValue(s"$idx", "batches", "rows"),
        CodeLines.debugHere,
        GroupByOutline.declare(veT.makeCVector(outputVarName)),
        CodeLines.debugHere,
        s"${outputVarName}_g[0] = ${outputVarName};",
        veT match {
          case VeString =>
            CodeLines.from(
              CodeLines.debugHere,
              CodeLines.from(s"std::vector<frovedis::words> ${outputVarName}_multi_words(batches)"),
              CodeLines.forLoop("b", "batches")({
                s"${outputVarName}_multi_words[b] = varchar_vector_to_words(input_${idx}_g[b])"
              }),
              CodeLines.debugHere,
              s"frovedis::words ${outputVarName}_merged = frovedis::merge_multi_words(${outputVarName}_multi_words);",
              s"words_to_varchar_vector(${outputVarName}_merged, ${outputVarName});"
            )
            .blockCommented(s"$idx")

          case veScalarType: VeScalarType =>
            CodeLines
              .from(
                GroupByOutline.initializeScalarVector(veScalarType, outputVarName, "rows"),
                "int o = 0;",
                CodeLines.forLoop("b", "batches") {
                  val inputInBatch = s"input_${idx}_g[b]"
                  val countInBatch = s"$inputInBatch->count"
                  CodeLines.from(
                    CodeLines.debugHere,
                    CodeLines.forLoop("i", countInBatch) {
                      CodeLines.from(
                        s"$outputVarName->data[o] = $inputInBatch->data[i];",
                        s"set_validity($outputVarName->validityBuffer, o, check_valid($inputInBatch->validityBuffer, i));",
                        "o++;"
                      )
                    }
                  )
                }
              )
              .blockCommented(s"$idx")
        }
      )
    })
  )

}
