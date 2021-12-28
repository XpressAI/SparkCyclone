package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector

object GenericJoiner {

  def populateVarChar(leftDict: String, conj: String, output: String): CodeLines =
    CodeLines.from(s"""words_to_varchar_vector(${leftDict}.index_to_words(${conj}), ${output});""")

  def populateScalar(
    outputName: String,
    conj: String,
    inputName: String,
    veScalarType: VeScalarType
  ): CodeLines =
    CodeLines.from(
      initializeScalarVector(veScalarType, outputName, s"${conj}.size()"),
      CodeLines.forLoop("i", s"${conj}.size()")(
        CodeLines.from(
          s"${outputName}->data[i] = ${inputName}->data[${conj}[i]];",
          s"set_validity(${outputName}->validityBuffer, i, check_valid(${inputName}->validityBuffer, ${conj}[i]));"
        )
      )
    )

  def produce: CodeLines = {
    val conj_first_output_varchar_vector = "conj_a_v"
    val conj_second_output_int_vector = "conj_x_var"
    val conj_third_output_double_vector = "conj_y_var"
    val left_varchar_vector = "x_a"
    val left_matching_num_vector = "x_b"
    val left_input_int_vector = "x_c"
    val right_varchar_vector = "y_a"
    val right_matching_num_vector = "y_b"
    val right_input_double_vector = "y_c"
    val output_varchar_vector = "o_a_var"
    val output_int_vector = "o_b_var"
    val output_double_vector = "o_c_var"
    val left_dict = "left_dict_var"
    val left_dict_indices = "a1_var"
    val string_left_idx = "a1_out_var"
    val num_left_idx = "b1_out_var"
    val string_right_idx = "a2_out_var"
    val num_right_idx = "b2_out_var"
    val left_words = "left_words_var"
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
      s"""nullable_varchar_vector *${left_varchar_vector},""",
      s"""nullable_bigint_vector *${left_matching_num_vector},""",
      s"""nullable_int_vector *${left_input_int_vector},""",
      s"""nullable_varchar_vector *${right_varchar_vector},""",
      s"""nullable_bigint_vector *${right_matching_num_vector},""",
      s"""nullable_double_vector *${right_input_double_vector},""",
      s"""nullable_varchar_vector *$output_varchar_vector,""",
      s"""nullable_int_vector *${output_int_vector},""",
      s"""nullable_double_vector *${output_double_vector})""",
      """{""",
      CodeLines
        .from(
          CodeLines
            .from(
              s"frovedis::words ${left_words} = varchar_vector_to_words($left_varchar_vector);",
              "frovedis::dict " + left_dict + s" = frovedis::make_dict(${left_words});"
            ),
          computeStringJoin(
            leftDictIndices = left_dict_indices,
            matchingIndicesLeft = string_left_idx,
            matchingIndicesRight = string_right_idx,
            leftDict = left_dict,
            leftWords = left_words,
            rightVec = right_varchar_vector
          ),
          computeNumJoin(
            leftOut = num_left_idx,
            leftInput = left_matching_num_vector,
            rightOut = num_right_idx,
            rightInput = right_matching_num_vector
          ),
          computeConjunction(
            colALeft = string_left_idx,
            colBLeft = num_left_idx,
            conj = List(
              Conj(
                conj_first_output_varchar_vector,
                s"${left_dict_indices}[${string_left_idx}[i]]"
              ),
              Conj(conj_second_output_int_vector, s"${string_left_idx}[i]"),
              Conj(conj_third_output_double_vector, s"${string_right_idx}[i]")
            ),
            pairings = List(
              EqualityPairing(string_left_idx, num_left_idx),
              EqualityPairing(string_right_idx, num_right_idx)
            )
          ),
          populateVarChar(
            leftDict = left_dict,
            conj = conj_first_output_varchar_vector,
            output = output_varchar_vector
          ),
          populateScalar(
            outputName = output_int_vector,
            conj = conj_second_output_int_vector,
            inputName = left_input_int_vector,
            veScalarType = VeScalarType.VeNullableInt
          ),
          populateScalar(
            outputName = output_double_vector,
            conj = conj_third_output_double_vector,
            inputName = right_input_double_vector,
            veScalarType = VeScalarType.VeNullableDouble
          ),
          "return 0;"
        )
        .indented,
      """}"""
    )
  }

  final case class Conj(name: String, input: String)

  final case class EqualityPairing(left: String, right: String) {
    def toCondition: String = s"$left[i] == $right[j]"
  }

  private def computeConjunction(
    colALeft: String,
    colBLeft: String,
    conj: List[Conj],
    pairings: List[EqualityPairing]
  ): CodeLines = {
    CodeLines
      .from(
        conj.map(n => s"std::vector<size_t> ${n.name};"),
        s"for (int i = 0; i < $colALeft.size(); i++) {",
        s"  for (int j = 0; j < $colBLeft.size(); j++) {",
        s"    if (${pairings.map(_.toCondition).mkString(" && ")}) {",
        conj.map { case Conj(k, i) => s"$k.push_back($i);" },
        "    }",
        "  }",
        "}"
      )
  }

  private def computeNumJoin(
    leftOut: String,
    rightOut: String,
    leftInput: String,
    rightInput: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${leftOut};",
      s"std::vector<size_t> ${rightOut};",
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
          s"frovedis::equi_join(right, right_idx, left, left_idx, $rightOut, $leftOut);"
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
