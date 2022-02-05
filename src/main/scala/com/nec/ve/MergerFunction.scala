package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.groupby.GroupByOutline

object MergerFunction {
  def mergeCVecStmt(vetype: VeType, index: Int): CodeLines = {
    val in = s"input_${index}_g"
    val out = s"output_${index}_g"
    val tmp = s"output_${index}"

    val mergeStmt = vetype match {
      case VeString =>
        CodeLines.from(
          // Declare std::vector<frovedis::words>
          s"std::vector<frovedis::words> ${tmp}_multi_words(batches);",
          // Loop over the batches
          CodeLines.forLoop("b", "batches") {
            // Copy each nullable_varchar_vector over
            s"${tmp}_multi_words[b] = varchar_vector_to_words(${in}[b]);"
          },
          "",
          // Perform merge using frovedis
          s"frovedis::words ${tmp}_merged = frovedis::merge_multi_words(${tmp}_multi_words);",
          // Convert back to nullable_varchar_vector
          s"words_to_varchar_vector(${tmp}_merged, ${tmp});",
          "",
          // Initialize index counter
          "auto o = 0;",
          CodeLines.forLoop("b", "batches") {
            CodeLines.forLoop("i", s"${in}[b]->count") {
              s"set_validity(${tmp}->validityBuffer, o++, check_valid(${in}[b]->validityBuffer, i));"
            }
          }
        )

      case scalar: VeScalarType =>
        CodeLines.from(
          // Initialize the mullable_T_vector
          GroupByOutline.initializeScalarVector(scalar, tmp, "rows"),
          "",
          // Initialize index counter
          "auto o = 0;",
          // Loop over the batches
          CodeLines.forLoop("b", "batches") {
            // Loop over all values in each batch
            CodeLines.forLoop("i", s"${in}[b]->count") {
              List(
                // Copy value over
                s"${tmp}->data[o] = ${in}[b]->data[i];",
                // Preserve validity bits across merges
                s"set_validity(${tmp}->validityBuffer, o++, check_valid(${in}[b]->validityBuffer, i));",
              )
            }
          }
        )
    }

    CodeLines.scoped(s"Merge ${in}[...] into ${out}[0]") {
      CodeLines.from(
        // Allocate the nullable_T_vector[]
        s"*${out} = (${vetype.cVectorType} *) malloc(sizeof(void *));",
        // Allocate new mullable_T_vector
        s"${out}[0] = (${vetype.cVectorType} *) malloc(sizeof(${vetype.cVectorType}));",
        // Set a temporary pointer
        s"auto *${tmp} = ${out}[0];",
        "",
        mergeStmt
      )
    }
  }

  def merge(types: List[VeType]): CFunction2 = {
    val inputs = types.zipWithIndex.map { case (veType, idx) =>
      CFunctionArgument.PointerPointer(veType.makeCVector(s"input_${idx}_g"))
    }

    val outputs = types.zipWithIndex.map { case (veType, idx) =>
      CFunctionArgument.PointerPointer(veType.makeCVector(s"output_${idx}_g"))
    }

    CFunction2(
      arguments = List(CFunctionArgument.Raw("int batches"), CFunctionArgument.Raw("int rows")) ++ inputs ++ outputs,
      body = types.zipWithIndex.map((mergeCVecStmt _).tupled)
    )
  }
}
