package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
import com.nec.spark.agile.join.GenericJoiner._

final case class GenericJoiner(
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  joins: List[Join],
  outputs: List[FilteredOutput]
) {

  private val joinByEquality = JoinByEquality(inputsLeft, inputsRight, joins)

  def toCombinedCodeLines(fName: String): CodeLines = {
    val computeIndicesFunctionName: String = s"compute_indices_${fName}"
    CodeLines.from(
      cFunctionExtra.toCodeLinesNoHeader(computeIndicesFunctionName),
      cFunction(computeIndicesFunctionName).toCodeLinesS(fName)
    )
  }

  def cFunctionExtra: CFunction = joinByEquality.produceIndices

  def cFunction(computeIndicesFunctionName: String): CFunction = CFunction(
    inputs = inputsLeft ++ inputsRight,
    outputs = outputs.map(_.cVector),
    body = CodeLines.from(
      "nullable_int_vector left_idx;",
      "nullable_int_vector right_idx;",
      s"${computeIndicesFunctionName}(${{
        joinByEquality.ioWo.map(_.name) ++
          joinByEquality.ioO.map(v => s"&${v.name}")
      }.mkString(", ")});",
      s"std::vector<size_t> left_idx_std = idx_to_std(&left_idx);",
      s"std::vector<size_t> right_idx_std = idx_to_std(&right_idx);",
      outputs.map {
        case FilteredOutput(newName, source @ CScalarVector(name, veType)) =>
          val isLeft = inputsLeft.contains(source)
          val indicesName = if (isLeft) "left_idx_std" else "right_idx_std"
          CodeLines.from(
            populateScalar(
              outputName = newName,
              inputIndices = indicesName,
              inputName = name,
              veScalarType = veType
            )
          )

        case FilteredOutput(outName, source @ CVarChar(name)) =>
          val isLeft = inputsLeft.contains(source)
          val indicesName = if (isLeft) "left_idx_std" else "right_idx_std"

          CodeLines.from(
            s"auto ${name}_words = ${name}->to_words();",
            s"auto ${name}_filtered_words = filter_words(${name}_words, ${indicesName});",
            s"""new (${outName}) nullable_varchar_vector(${name}_filtered_words);"""
          )
      }
    )
  )

  def produce(fName: String): CodeLines = toCombinedCodeLines(fName)
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
          s"set_validity(${outputName}->validityBuffer, i, check_valid(${inputName}->validityBuffer, ${inputIndices}[i]));"
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
      CodeLines
        .from(
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
        .block
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
      CodeLines
        .from(
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
        .block
    )

  def printVec: CodeLines = CodeLines.from(
    """#ifdef DEBUG""",
    """template<typename T>""",
    """void print_vec(char *name, std::vector<T> a) {""",
    CodeLines
      .from(
        """std::cout << name << " = [";""",
        """char *comma = "";""",
        """for (int i = 0; i < a.size(); i++) {""",
        """std::cout << comma << a[i];""",
        """comma = ",";""",
        """}""",
        """std::cout << "]" << std::endl;"""
      )
      .indented,
    """}""",
    """#endif"""
  )
}
