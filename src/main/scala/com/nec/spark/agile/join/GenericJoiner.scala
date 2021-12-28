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
    val conj_a_var = "conj_a_v"
    val conj_x_var = "conj_x_var"
    val conj_y_var = "conj_y_var"
    val x_a = "x_a"
    val x_b = "x_b"
    val x_c = "x_c"
    val y_a = "y_a"
    val y_b = "y_b"
    val y_c = "y_c"
    val o_a_var = "o_a_var"
    val o_b = "o_b_var"
    val o_c = "o_c_var"
    val left_dict_var = "left_dict_var"
    val a1_var = "a1_var"
    val a1_out_var = "a1_out_var"
    val b1_out_var = "b1_out_var"
    val a2_out_var = "a2_out_var"
    val b2_out_var = "b2_out_var"
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
      s"""nullable_varchar_vector *${x_a},""",
      s"""nullable_bigint_vector *${x_b},""",
      s"""nullable_int_vector *${x_c},""",
      s"""nullable_varchar_vector *${y_a},""",
      s"""nullable_bigint_vector *${y_b},""",
      s"""nullable_double_vector *${y_c},""",
      s"""nullable_varchar_vector *$o_a_var,""",
      s"""nullable_int_vector *${o_b},""",
      s"""nullable_double_vector *${o_c})""",
      """{""",
      CodeLines
        .from(
          CodeLines
            .from(
              s"frovedis::words ${left_words} = varchar_vector_to_words($x_a);",
              "frovedis::dict " + left_dict_var + s" = frovedis::make_dict(${left_words});"
            ),
          computeStringJoin(
            leftOutIndices = a1_var,
            leftOut = a1_out_var,
            rightOut = a2_out_var,
            leftDict = left_dict_var,
            leftWords = left_words,
            rightVec = y_a
          ),
          computeNumJoin(
            leftOut = b1_out_var,
            rightOut = b2_out_var,
            leftInput = x_b,
            rightInput = y_b
          ),
          computeConjunction(
            colALeft = a1_out_var,
            colARight = b1_out_var,
            conj = List(
              Conj(conj_a_var, s"${a1_var}[${a1_out_var}[i]]"),
              Conj(conj_x_var, s"${a1_out_var}[i]"),
              Conj(conj_y_var, s"${a2_out_var}[i]")
            ),
            pairings =
              List(EqualityPairing(a1_out_var, b1_out_var), EqualityPairing(a2_out_var, b2_out_var))
          ),
          populateVarChar(left_dict_var, conj_a_var, o_a_var),
          populateScalar(o_b, conj_x_var, x_c, VeScalarType.VeNullableInt),
          populateScalar(o_c, conj_y_var, y_c, VeScalarType.VeNullableDouble),
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
    colARight: String,
    conj: List[Conj],
    pairings: List[EqualityPairing]
  ): CodeLines = {
    CodeLines
      .from(
        conj.map(n => s"std::vector<size_t> ${n.name};"),
        s"for (int i = 0; i < $colALeft.size(); i++) {",
        s"  for (int j = 0; j < $colARight.size(); j++) {",
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
