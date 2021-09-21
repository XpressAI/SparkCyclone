package com.nec.spark.planning
import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.agile.CExpressionEvaluation.{CodeLines, RichListStr}
import org.apache.spark.sql.catalyst.expressions.Attribute

object NewCExpressionEvaluation {
  def evaluate(
    fName: String,
    output: Seq[Attribute],
    beginIndex: Int,
    endIndex: Int
  ): CExpressionEvaluation.CodeLines = {
    CodeLines.from(
      """#include <stdio.h>""",
      """#include <stdlib.h>""",
      """#include <string.h>""",
      """#include <iostream>""",
      """#include <string>""",
      """#include "cpp/frovedis/text/words.hpp"""",
      s"""extern "C" long ${fName}(non_null_varchar_vector* input_strings, non_null_varchar_vector* output_strings, nullable_int_vector* lengths, non_null_varchar_vector* output_strings_2) {""",
      CodeLines
        .from(
          "int length = input_strings->offsets[input_strings->count];",
          """std::string output_result("");""",
          """std::string output_result_2("");""",
          """std::string input_str(input_strings->data, input_strings->size);""",
          """std::vector<int> output_offsets;""",
          """std::vector<int> output_offsets_2;""",
          """int currentOffset = 0;""",
          """int currentOffset_2 = 0;""",
          """for ( int i = 0; i < input_strings->count; i++ ) {""",
          s"""    for ( int j = ${beginIndex}; j < ${endIndex}; j++ ) {""",
          """        output_result.append((input_strings->data + (input_strings->offsets[i] + j)), 1);""",
          """    }""",
          s"int beginIndex_2 = 1;",
          s"int string_i_length = input_strings->offsets[i + 1] - input_strings->offsets[i];",
          s"int endIndex_2 = string_i_length - 2;",
          s"""    for ( int j = beginIndex_2; j < endIndex_2; j++ ) {""",
          """        output_result_2.append((input_strings->data + (input_strings->offsets[i] + j)), 1);""",
          """    }""",
          """    output_offsets.push_back(currentOffset);""",
          """    output_offsets_2.push_back(currentOffset_2);""",
          s"""    currentOffset += (${endIndex} - ${beginIndex});""",
          s"""    currentOffset_2 += (endIndex_2 - beginIndex_2);""",
          """}""",
          """output_offsets.push_back(currentOffset);""",
          """output_offsets_2.push_back(currentOffset_2);""",
          """output_strings->count = input_strings->count;""",
          """output_strings_2->count = input_strings->count;""",
          """output_strings->size = currentOffset;""",
          """output_strings_2->size = currentOffset_2;""",
          """output_strings->data = (char*)malloc(output_strings->size);""",
          """output_strings_2->data = (char*)malloc(output_strings_2->size);""",
          """memcpy(output_strings->data, output_result.data(), output_strings->size);""",
          """memcpy(output_strings_2->data, output_result_2.data(), output_strings_2->size);""",
          """output_strings->offsets = (int*)malloc(4 * (output_strings->count + 1));""",
          """output_strings_2->offsets = (int*)malloc(4 * (output_strings_2->count + 1));""",
          """memcpy(output_strings->offsets, output_offsets.data(), 4 * (output_strings->count + 1));""",
          """memcpy(output_strings_2->offsets, output_offsets_2.data(), 4 * (output_strings_2->count + 1));""",
          s"lengths->data = (int*) malloc(input_strings->count * 4);",
          s"lengths->validityBuffer = (unsigned char *) malloc(input_strings->count * 4);",
          s"lengths->count = input_strings->count;",
          s"for( int i = 0; i < input_strings->count; i++ ) {",
          "  lengths->data[i] = input_strings->offsets[i + 1] - input_strings->offsets[i];",
          "  set_validity(lengths->validityBuffer, i, 1);",
          "}",
          """return 0;"""
        )
        .indented,
      "}"
    )
  }
}
