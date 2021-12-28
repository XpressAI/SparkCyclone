package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeScalarType, VeString, VeType}
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector

object GenericJoiner {

  def populateVarChar(leftDict: String, inputIndices: String, outputName: String): CodeLines =
    CodeLines.from(
      s"""words_to_varchar_vector(${leftDict}.index_to_words(${inputIndices}), ${outputName});"""
    )

  def populateScalar(
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

  final case class Join(left: CVector, right: CVector)

  def produce: CodeLines = {
    val left_input_int_vector = "x_c"
    val right_input_double_vector = "y_c"
    val left_dict_indices = "a1_var"
    val string_match_left_idx = "a1_out_var"
    val num_match_left_idx = "b1_out_var"
    val string_right_idx = "a2_out_var"
    val num_right_idx = "b2_out_var"

    val inputsLeft =
      List(CVector.varChar("x_a"), CVector.bigInt("x_b"), CVector.int(left_input_int_vector))

    val inputsRight =
      List(CVector.varChar("y_a"), CVector.bigInt("y_b"), CVector.double(right_input_double_vector))

    val firstJoin = Join(left = inputsLeft(0), right = inputsRight(0))
    val secondJoin = Join(left = inputsLeft(1), right = inputsRight(1))

    val left_words = s"words_${firstJoin.left.name}"
    val left_dict = s"dict_${firstJoin.left.name}"

    val dictsWords = CodeLines
      .from(
        s"frovedis::words ${left_words} = varchar_vector_to_words(${firstJoin.left.name});",
        s"frovedis::dict ${left_dict} = frovedis::make_dict(${left_words});"
      )

    val outputs: List[Output] = List(
      outStr(
        outputName = "o_a_var",
        sourceDict = left_dict,
        sourceDictIndices = left_dict_indices,
        sourceDictIndicesIndices = string_match_left_idx
      ),
      outScalar(
        source = left_input_int_vector,
        sourceIndices = string_match_left_idx,
        outputName = "o_b_var",
        veScalarType = VeScalarType.VeNullableInt
      ),
      outScalar(
        source = right_input_double_vector,
        sourceIndices = string_right_idx,
        outputName = "o_c_var",
        veScalarType = VeScalarType.VeNullableDouble
      )
    )

    val joins = CodeLines.from(
      computeStringJoin(
        leftDictIndices = left_dict_indices,
        matchingIndicesLeft = string_match_left_idx,
        matchingIndicesRight = string_right_idx,
        leftDict = left_dict,
        leftWords = left_words,
        rightVec = firstJoin.right.name
      ),
      computeNumJoin(
        matchingIndicesLeft = num_match_left_idx,
        leftInput = secondJoin.left.name,
        matchingIndicesRight = num_right_idx,
        rightInput = secondJoin.right.name
      )
    )

    val conjunctions = computeConjunction(
      firstLeft = string_match_left_idx,
      secondLeft = num_match_left_idx,
      conj = outputs.map(_.conj),
      pairings = List(
        EqualityPairing(string_match_left_idx, num_match_left_idx),
        EqualityPairing(string_right_idx, num_right_idx)
      )
    )

    val inputs = inputsLeft ++ inputsRight

    CodeLines.from(
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/dataframe/join.cc"""",
      """#include "frovedis/text/words.hpp"""",
      """#include "frovedis/text/words.cc"""",
      """#include "frovedis/text/dict.hpp"""",
      """#include "frovedis/text/dict.cc"""",
      """#include <iostream>""",
      """#include <vector>""",
      """#include <cmath>""",
      printVec,
      """extern "C" long adv_join(""",
      (inputs ++ outputs
        .map(_.cVector))
        .map(_.declarePointer)
        .mkString(",\n"),
      ")",
      """{""",
      CodeLines
        .from(dictsWords, joins, conjunctions, outputs.map(_.outputProduction), "return 0;")
        .indented,
      """}"""
    )
  }

  final case class Output(
    outputName: String,
    conj: Conj,
    outputProduction: CodeLines,
    veType: VeType
  ) {
    def cVector: CVector = CVector(outputName, veType)
  }

  private def outScalar(
    source: String,
    outputName: String,
    sourceIndices: String,
    veScalarType: VeScalarType
  ): Output = {

    val conj_name = s"conj_${outputName}"

    Output(
      outputName = outputName,
      conj = Conj(conj_name, s"${sourceIndices}[i]"),
      outputProduction = populateScalar(
        outputName = outputName,
        inputIndices = conj_name,
        inputName = source,
        veScalarType = veScalarType
      ),
      veType = veScalarType
    )

  }

  private def outStr(
    outputName: String,
    sourceDict: String,
    sourceDictIndices: String,
    sourceDictIndicesIndices: String
  ): Output = {
    val conj_name = s"conj_${outputName}"
    Output(
      outputName = outputName,
      conj = Conj(conj_name, s"${sourceDictIndices}[${sourceDictIndicesIndices}[i]]"),
      outputProduction =
        populateVarChar(leftDict = sourceDict, inputIndices = conj_name, outputName = outputName),
      veType = VeString
    )
  }

  final case class Conj(name: String, input: String)

  final case class EqualityPairing(indexOfFirstColumn: String, indexOfSecondColumn: String) {
    def toCondition: String = s"$indexOfFirstColumn[i] == $indexOfSecondColumn[j]"
  }

  /**
   * This combines joins from multiple places to produce corresponding indices for output items
   */
  private def computeConjunction(
    firstLeft: String,
    secondLeft: String,
    conj: List[Conj],
    pairings: List[EqualityPairing]
  ): CodeLines =
    CodeLines
      .from(
        conj.map(n => s"std::vector<size_t> ${n.name};"),
        s"for (int i = 0; i < $firstLeft.size(); i++) {",
        s"  for (int j = 0; j < $secondLeft.size(); j++) {",
        s"    if (${pairings.map(_.toCondition).mkString(" && ")}) {",
        conj.map { case Conj(k, i) => s"$k.push_back($i);" },
        "    }",
        "  }",
        "}"
      )

  private def computeNumJoin(
    matchingIndicesLeft: String,
    matchingIndicesRight: String,
    leftInput: String,
    rightInput: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${matchingIndicesLeft};",
      s"std::vector<size_t> ${matchingIndicesRight};",
      CodeLines
        .from(
          s"std::vector<int64_t> left(${leftInput}->count);",
          s"std::vector<size_t> left_idx(${leftInput}->count);",
          s"for (int i = 0; i < ${leftInput}->count; i++) {",
          s"  left[i] = ${leftInput}->data[i];",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<int64_t> right(${rightInput}->count);",
          s"std::vector<size_t> right_idx(${rightInput}->count);",
          s"for (int i = 0; i < ${rightInput}->count; i++) {",
          s"  right[i] = ${rightInput}->data[i];",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, left, left_idx, $matchingIndicesRight, $matchingIndicesLeft);"
        )
        .block
    )

  private def computeStringJoin(
    leftWords: String,
    leftDict: String,
    leftDictIndices: String,
    matchingIndicesLeft: String,
    matchingIndicesRight: String,
    rightVec: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${matchingIndicesLeft};",
      s"std::vector<size_t> ${matchingIndicesRight};",
      s"std::vector<size_t> ${leftDictIndices} = $leftDict.lookup(frovedis::make_compressed_words(${leftWords}));",
      CodeLines
        .from(
          s"std::vector<size_t> left_idx($leftDictIndices.size());",
          s"for (int i = 0; i < $leftDictIndices.size(); i++) {",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<size_t> right = $leftDict.lookup(frovedis::make_compressed_words(varchar_vector_to_words(${rightVec})));",
          s"std::vector<size_t> right_idx(right.size());",
          s"for (int i = 0; i < right.size(); i++) {",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, $leftDictIndices, left_idx, ${matchingIndicesRight}, ${matchingIndicesLeft});"
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
