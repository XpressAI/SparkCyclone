package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline
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
    val x_a = "x_a"
    val x_b = "x_b"
    val x_c = "x_c"
    val y_a = "y_a"
    val y_b = "y_b"
    val y_c = "y_c"
    val o_a = "o_a"
    val o_b = "o_b"
    val o_c = "o_c"
    val populateFirstColumn = populateVarChar("left_dict", "conj_a", "o_a")
    val populateSecondColumn = populateScalar("o_b", "conj_x", "x_c", VeScalarType.VeNullableInt)
    val populateThirdColumn = populateScalar("o_c", "conj_y", "y_c", VeScalarType.VeNullableDouble)

    val computeA1OutA2Out =
      computeStringJoin(
        leftOutIndices = "a1",
        leftOut = "a1_out",
        rightOut = "a2_out",
        leftDict = "left_dict",
        leftWords = "left_words",
        rightVec = "y_a"
      )
    val computeB1B2Out =
      computeNumJoin("b1_out", "b2_out", "x_b", "y_b")

    val computeConj = compCC(
      conj_a = "conj_a",
      conj_x = "conj_x",
      conj_y = "conj_y",
      a1_out = "a1_out",
      b1_out = "b1_out",
      a2_out = "a2_out",
      b2_out = "b2_out",
      a1 = "a1",
    )
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
      s"""nullable_varchar_vector *${x_a},""",
      s"""nullable_bigint_vector *${x_b},""",
      s"""nullable_int_vector *${x_c},""",
      s"""nullable_varchar_vector *${y_a},""",
      s"""nullable_bigint_vector *${y_b},""",
      s"""nullable_double_vector *${y_c},""",
      s"""nullable_varchar_vector *${o_a},""",
      s"""nullable_int_vector *${o_b},""",
      s"""nullable_double_vector *${o_c})""",
      """{""",
      CodeLines
        .from(
          "frovedis::words left_words = varchar_vector_to_words(x_a);",
          "frovedis::dict left_dict = frovedis::make_dict(left_words);"
        ),
      computeA1OutA2Out,
      computeB1B2Out,
      computeConj,
      CodeLines
        .from(populateFirstColumn, populateSecondColumn, populateThirdColumn, "return 0;")
        .indented,
      """}"""
    )
  }

  private def compCC(
    conj_a: String,
    conj_x: String,
    conj_y: String,
    a1_out: String,
    b1_out: String,
    a2_out: String,
    b2_out: String,
    a1: String,
  ) = {
    CodeLines
      .from(
        s"std::vector<size_t> $conj_a;",
        s"std::vector<size_t> $conj_x;",
        s"std::vector<size_t> $conj_y;",
        s"for (int i = 0; i < $a1_out.size(); i++) {",
        s"  for (int j = 0; j < $b1_out.size(); j++) {",
        s"    if ($a1_out[i] == $b1_out[j] && $a2_out[i] == $b2_out[j]) {",
        s"      $conj_a.push_back($a1[$a1_out[i]]);",
        s"      $conj_x.push_back($a1_out[i]);",
        s"      $conj_y.push_back($a2_out[i]);",
        "    }",
        "  }",
        "}"
      )
  }

  private def computeNumJoin(
    b1_out: String,
    b2_out: String,
    leftInput: String,
    rightInput: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${b1_out};",
      s"std::vector<size_t> ${b2_out};",
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
          s"frovedis::equi_join(right, right_idx, left, left_idx, $b2_out, $b1_out);"
        )
        .block
    )

  private def computeStringJoin(
    leftWords: String,
    leftDict: String,
    leftOutIndices: String,
    leftOut: String,
    rightOut: String,
    rightVec: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${leftOut};",
      s"std::vector<size_t> ${rightOut};",
      s"std::vector<size_t> ${leftOutIndices} = $leftDict.lookup(frovedis::make_compressed_words(${leftWords}));",
      CodeLines
        .from(
          s"std::vector<size_t> left_idx($leftOutIndices.size());",
          s"for (int i = 0; i < $leftOutIndices.size(); i++) {",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<size_t> right = $leftDict.lookup(frovedis::make_compressed_words(varchar_vector_to_words(${rightVec})));",
          s"std::vector<size_t> right_idx(right.size());",
          s"for (int i = 0; i < right.size(); i++) {",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, $leftOutIndices, left_idx, ${rightOut}, ${leftOut});"
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
