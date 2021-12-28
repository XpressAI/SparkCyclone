package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector

object GenericJoiner {
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
          """
            |    std::vector<size_t> a1_out;
            |    std::vector<size_t> a2_out;
            |
            |    frovedis::words left_words = varchar_vector_to_words(x_a);
            |    frovedis::words right_words = varchar_vector_to_words(y_a);
            |
            |    frovedis::compressed_words left_cwords = frovedis::make_compressed_words(left_words);
            |    frovedis::compressed_words right_cwords = frovedis::make_compressed_words(right_words);
            |
            |    frovedis::dict left_dict = frovedis::make_dict(left_words);
            |    frovedis::dict right_dict = frovedis::make_dict(right_words);
            |
            |    // First = => a1 = a2
            |    std::vector<size_t> a1 = left_dict.lookup(left_cwords);
            |    std::vector<size_t> a1_idx(a1.size());
            |    for (int i = 0; i < a1.size(); i++) {
            |        a1_idx[i] = i;
            |    }
            |
            |    std::vector<size_t> a2 = left_dict.lookup(right_cwords);
            |    std::vector<size_t> a2_idx(a2.size());
            |    for (int i = 0; i < a2.size(); i++) {
            |        a2_idx[i] = i;
            |    }
            |
            |    // start with smaller vector
            |    frovedis::equi_join(a2, a2_idx, a1, a1_idx, a2_out, a1_out);
            |
            |    // Second = => b1 = b2
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
            |
            |    std::vector<size_t> b1_out;
            |    std::vector<size_t> b2_out;
            |    frovedis::equi_join(b2, b2_idx, b1, b1_idx, b2_out, b1_out);
            |
            |    // Resolve && in a1 == a2 && b1 == b2
            |    // Conjunction is where the resulting outs are the same.
            |    // 1 -> 0, 1 -> 1, 3 -> 3
            |    // TODO: this could be vectorized to a compress operation.  Need to check.
            |    std::vector<size_t> conj_x;
            |    std::vector<size_t> conj_y;
            |    for (int i = 0; i < a1_out.size(); i++) {
            |        for (int j = 0; j < b1_out.size(); j++) {
            |            if (a1_out[i] == b1_out[j]) {
            |                if (a2_out[i] == b2_out[j]) {
            |                    conj_x.push_back(a1_out[i]);
            |                    conj_y.push_back(a2_out[i]);
            |                }
            |            }
            |        }
            |    }
            |
            |    std::vector<size_t> conj_a;
            |    for (int i = 0; i < conj_x.size(); i++) {
            |        conj_a.push_back(a1[conj_x[i]]);
            |    }
            |
            |    // TODO: need to double check this. b is left_dict.lookup(right_cwords)
            |    frovedis::words result = left_dict.index_to_words(conj_a);
            |    words_to_varchar_vector(result, o_a);
            |
            |    o_b->count = conj_x.size();
            |    o_b->data = (int32_t *)malloc(o_b->count * sizeof(int32_t));
            |    o_b->validityBuffer = (uint64_t *)malloc(ceil(o_b->count / 64.0) * sizeof(uint64_t));
            |    for (int i = 0; i < conj_x.size(); i++) {
            |        o_b->data[i] = x_c->data[conj_x[i]];
            |        set_validity(o_b->validityBuffer, i, check_valid(x_c->validityBuffer, conj_x[i]));
            |    }
            |
            |    o_c->count = conj_y.size();
            |    o_c->data = (double *)malloc(o_c->count * sizeof(double));
            |    o_c->validityBuffer = (uint64_t *)malloc(ceil(o_c->count / 64.0) * sizeof(uint64_t));
            |    for (int i = 0; i < conj_y.size(); i++) {
            |        o_c->data[i] = y_c->data[conj_y[i]];
            |        set_validity(o_c->validityBuffer, i, check_valid(y_c->validityBuffer, conj_y[i]));
            |    }
            |""".stripMargin,

          initializeScalarVector(VeScalarType.VeNullableDouble, "o_c", "conj_y.size()"),
          CodeLines.forLoop("i", "conj_y.size()")(
            CodeLines.from(
              s"o_c->data[i] = y_c->data[conj_y[i]];",
              "set_validity(o_c->validityBuffer, i, check_valid(y_c->validityBuffer, conj_y[i]));"
            )
          ),
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
