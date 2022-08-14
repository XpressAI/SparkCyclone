package io.sparkcyclone.spark.codegen.join

import io.sparkcyclone.native.code._
import io.sparkcyclone.native.code.CFunction2.CFunctionArgument
import io.sparkcyclone.spark.codegen.CFunctionGeneration._
import io.sparkcyclone.native.code.CFunction2
import io.sparkcyclone.spark.codegen.groupby.GroupByOutline.initializeScalarVector
import io.sparkcyclone.spark.codegen.join.GenericJoiner.{FilteredOutput, _}

final case class GenericJoiner(
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  joins: List[Join],
  outputs: List[FilteredOutput]
) {

  private val joinByEquality = JoinByEquality(inputsLeft, inputsRight, joins)

  def cFunctionExtra: CFunction = joinByEquality.produceIndices

  lazy val fn_inputs: List[CVector] = {
    (inputsLeft ++ inputsRight).map { cVector =>
      cVector.veType.makeCVector(s"${cVector.name}_m")
    }
  }

  lazy val fn_outputs: List[CVector] = {
    outputs.map { filteredOutput =>
      filteredOutput.cVector.veType.makeCVector(s"${filteredOutput.cVector.name}_mo")
    }
  }

  lazy val arguments: List[CFunctionArgument] = {
    List(
      CFunctionArgument.Raw("size_t leftBatches"),
      CFunctionArgument.Raw("size_t rightBatches"),
      CFunctionArgument.Raw("size_t leftRows"),
      CFunctionArgument.Raw("size_t rightRows")
    ) ++ fn_inputs.map(CFunctionArgument.PointerPointer) ++ fn_outputs.map(CFunctionArgument.PointerPointer)
  }

  def cFunction(fnName: String,
                computeIndicesFunctionName: String): CFunction2 = CFunction2(
    name = fnName,
    arguments = arguments,
    body = CodeLines.from(
      mergeInputBatches,
      outputs.map{ filteredOutput =>
        CodeLines.from(
          s"${filteredOutput.cVector.veType.cVectorType} *${filteredOutput.cVector.name} = ${filteredOutput.cVector.veType.cVectorType}::allocate();",
          s"*${filteredOutput.cVector.name}_mo = ${filteredOutput.cVector.name};"
        )
      },
      "nullable_int_vector left_idx;",
      "nullable_int_vector right_idx;",
      s"${computeIndicesFunctionName}(${
        {
          joinByEquality.ioWo.map(_.name) ++
            joinByEquality.ioO.map(v => s"&${v.name}")
        }.mkString(", ")
      });",
      s"const auto left_idx_std = left_idx.size_t_data_vec();",
      s"const auto right_idx_std = right_idx.size_t_data_vec();",
      outputs.map {
        case FilteredOutput(output, source) =>
          val indicesName = if (inputsLeft.contains(source)) "left_idx_std" else "right_idx_std"
          CodeLines.from(s"${output}->move_assign_from(${source.name}->select(${indicesName}));")
      },
    )
  )


  /**
   * Merge input batches into single batch
   */
  private def mergeInputBatches: CodeLines = {
    def merge(prefix: String, input: CVector) = CodeLines.from(
      // Merge inputs
      s"${input.veType.cVectorType}* ${input.name} = ${input.veType.cVectorType}::merge(${input.name}_m, ${prefix}Batches);",
    )

    CodeLines.from(
      inputsLeft.map(merge("left", _)),
      inputsRight.map(merge("right", _)),
      "",
    )
  }
}

object GenericJoiner {

  final case class FilteredOutput(outputName: String, source: CVector) {
    def cVector: CVector = source.withNewName(outputName)
  }

  private def populateScalar(
    outputName: String,
    inputIndices: String,
    inputName: String,
    veScalarType: VeScalarType
  ): CodeLines =
    CodeLines.from(
      initializeScalarVector(veScalarType, outputName, s"${inputIndices}.size()"),
      CodeLines.forLoop("i", s"${inputIndices}.size()")(
        CodeLines.from(
          s"${outputName}->data[i] = ${inputName}->data[${inputIndices}[i]];",
          s"${outputName}->set_validity(i, ${inputName}->get_validity(${inputIndices}[i]));"
        )
      )
    )

  final case class Join(left: CVector, right: CVector) {
    def vecs: List[CVector] = List(left, right)
  }

  final case class EqualityPairing(indexOfFirstColumn: String, indexOfSecondColumn: String) {
    def toCondition: String = s"$indexOfFirstColumn[i] == $indexOfSecondColumn[j]"
  }

  def computeNumJoin(
    outMatchingIndicesLeft: String,
    outMatchingIndicesRight: String,
    inLeft: String,
    inRight: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${outMatchingIndicesLeft};",
      s"std::vector<size_t> ${outMatchingIndicesRight};",
      CodeLines.scoped("Compute non-string join") {
        CodeLines.from(
          s"std::vector<int64_t> left(${inLeft}->count);",
          s"std::vector<size_t> left_idx(${inLeft}->count);",
          CodeLines.forLoop("i", s"${inLeft}->count") {
            List(s"left[i] = ${inLeft}->data[i];", s"left_idx[i] = i;")
          },
          s"std::vector<int64_t> right(${inRight}->count);",
          s"std::vector<size_t> right_idx(${inRight}->count);",
          CodeLines.forLoop("i", s"${inRight}->count") {
            List(s"right[i] = ${inRight}->data[i];", s"right_idx[i] = i;")
          },
          s"frovedis::equi_join(right, right_idx, left, left_idx, $outMatchingIndicesRight, $outMatchingIndicesLeft);"
        )
      }
    )

  def computeStringJoin(
    inLeftWords: String,
    inLeftDict: String,
    leftDictIndices: String,
    outMatchingIndicesLeft: String,
    outMatchingIndicesRight: String,
    inRightVarChar: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${outMatchingIndicesLeft};",
      s"std::vector<size_t> ${outMatchingIndicesRight};",
      s"std::vector<size_t> ${leftDictIndices} = $inLeftDict.lookup(frovedis::make_compressed_words(${inLeftWords}));",
      CodeLines.scoped("Compute string join") {
        CodeLines.from(
          s"std::vector<size_t> left_idx($leftDictIndices.size());",
          s"for (int i = 0; i < $leftDictIndices.size(); i++) {",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<size_t> right = $inLeftDict.lookup(frovedis::make_compressed_words(${inRightVarChar}->to_words()));",
          s"std::vector<size_t> right_idx(right.size());",
          s"for (int i = 0; i < right.size(); i++) {",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, $leftDictIndices, left_idx, ${outMatchingIndicesRight}, ${outMatchingIndicesLeft});"
        )
      }
    )
}
