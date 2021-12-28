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

    val computeA1OutA2Out = CodeLines.from(
      """
        |    std::vector<size_t> a1_out;
        |    std::vector<size_t> a2_out;
        |    std::vector<size_t> a1 = left_dict.lookup(frovedis::make_compressed_words(left_words));
        |   {
        |   std::vector<size_t> a1_idx(a1.size());
        |    for (int i = 0; i < a1.size(); i++) {
        |        a1_idx[i] = i;
        |    }
        |    std::vector<size_t> a2 = left_dict.lookup(frovedis::make_compressed_words(varchar_vector_to_words(y_a)));
        |    std::vector<size_t> a2_idx(a2.size());
        |    for (int i = 0; i < a2.size(); i++) {
        |        a2_idx[i] = i;
        |    }
        |    frovedis::equi_join(a2, a2_idx, a1, a1_idx, a2_out, a1_out);
        |}
        |""".stripMargin
    )
    val computeB1B2Out =
      CodeLines.from("""
                       |    std::vector<size_t> b1_out;
                       |    std::vector<size_t> b2_out;
                       |     {
                       |    std::vector<int64_t> b1(x_b->count);
                       |    std::vector<size_t> b1_idx(x_b->count);
                       |    for (int i = 0; i < x_b->count; i++) {
                       |        b1[i] = x_b->data[i];
                       |        b1_idx[i] = i;
                       |    }
                       |    std::vector<int64_t> b2(y_b->count);
                       |    std::vector<size_t> b2_idx(y_b->count);
                       |    for (int i = 0; i < y_b->count; i++) {
                       |        b2[i] = y_b->data[i];
                       |        b2_idx[i] = i;
                       |    }
                       |    frovedis::equi_join(b2, b2_idx, b1, b1_idx, b2_out, b1_out);
                       |}""".stripMargin)
    val computeConj = CodeLines
      .from("""
              |    std::vector<size_t> conj_a;
              |    std::vector<size_t> conj_x;
              |    std::vector<size_t> conj_y;
              |    for (int i = 0; i < a1_out.size(); i++) {
              |        for (int j = 0; j < b1_out.size(); j++) {
              |            if (a1_out[i] == b1_out[j] && a2_out[i] == b2_out[j]) {
              |                conj_a.push_back(a1[a1_out[i]]);
              |                conj_x.push_back(a1_out[i]);
              |                conj_y.push_back(a2_out[i]);
              |            }
              |        }
              |    }
              |""".stripMargin)
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
        .from("""
                |    frovedis::words left_words = varchar_vector_to_words(x_a);
                |    frovedis::dict left_dict = frovedis::make_dict(left_words);
                |    
                |    """.stripMargin),
      computeA1OutA2Out,
      computeB1B2Out,
      computeConj,
      CodeLines
        .from(
          populateFirstColumn,
          populateSecondColumn,
          populateThirdColumn,
          """
            |    return 0;""".stripMargin
        )
        .indented,
      """}"""
    )
  }

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
