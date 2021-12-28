package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector

object GenericJoiner {

  def populateVarChar(leftDict: String, inputIndices: String, output: String): CodeLines =
    CodeLines.from(
      s"""words_to_varchar_vector(${leftDict}.index_to_words(${inputIndices}), ${output});"""
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

  def produce: CodeLines = {
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
    val string_match_left_idx = "a1_out_var"
    val num_match_left_idx = "b1_out_var"
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
            matchingIndicesLeft = string_match_left_idx,
            matchingIndicesRight = string_right_idx,
            leftDict = left_dict,
            leftWords = left_words,
            rightVec = right_varchar_vector
          ),
          computeNumJoin(
            leftOut = num_match_left_idx,
            leftInput = left_matching_num_vector,
            rightOut = num_right_idx,
            rightInput = right_matching_num_vector
          ), {

            final case class StringOutput(leftDict: String, leftDictIndices: String, outputName: String)
            val outputs: List[(Conj, CodeLines)] = List(
              {
                val conj_name = "conj_a_v"
                Conj(
                  conj_name,
                  s"${left_dict_indices}[${string_match_left_idx}[i]]"
                ) -> populateVarChar(
                  leftDict = left_dict,
                  inputIndices = conj_name,
                  output = output_varchar_vector
                )
              },
              {
                val conj_name = "conj_x_var"
                Conj(conj_name, s"${string_match_left_idx}[i]") -> populateScalar(
                  outputName = output_int_vector,
                  inputIndices = conj_name,
                  inputName = left_input_int_vector,
                  veScalarType = VeScalarType.VeNullableInt
                )
              },
              {
                val conj_name = "conj_y_var"

                Conj(conj_name, s"${string_right_idx}[i]") -> populateScalar(
                  outputName = output_double_vector,
                  inputIndices = conj_name,
                  inputName = right_input_double_vector,
                  veScalarType = VeScalarType.VeNullableDouble
                )
              }
            )
            CodeLines.from(
              computeConjunction(
                colALeft = string_match_left_idx,
                colBLeft = num_match_left_idx,

                conj = outputs.map(_._1),
                pairings = List(
                  EqualityPairing(string_match_left_idx, num_match_left_idx),
                  EqualityPairing(string_right_idx, num_right_idx)
                )
              ),
              outputs.map(_._2)
            )
          },
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
