package com.nec.spark.planning

import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.apache.spark.sql.catalyst.expressions.Attribute

object StringCExpressionEvaluation {
  def evaluate(
    fName: String,
    output: Seq[Attribute],
    beginIndex: Int,
    endIndex: Int
  ): CExpressionEvaluation.CodeLines = {

    val firstOutput = CodeLines
      .from(
        " // first output ",
        """std::string output_result("");""",
        """std::vector<int> output_offsets;""",
        """int currentOffset = 0;""",
        """for ( int i = 0; i < input_strings->count; i++ ) {""",
        CodeLines
          .from(
            s"""for ( int j = ${beginIndex}; j < ${endIndex}; j++ ) {""",
            CodeLines
              .from(
                """output_result.append((input_strings->data + (input_strings->offsets[i] + j)), 1);"""
              )
              .indented,
            "}",
            """output_offsets.push_back(currentOffset);""",
            s"""currentOffset += (${endIndex} - ${beginIndex});"""
          )
          .indented,
        """}""",
        """output_offsets.push_back(currentOffset);""",
        """output_strings->count = input_strings->count;""",
        """output_strings->size = currentOffset;""",
        """output_strings->data = (char*)malloc(output_strings->size);""",
        """memcpy(output_strings->data, output_result.data(), output_strings->size);""",
        """output_strings->offsets = (int*)malloc(4 * (output_strings->count + 1));""",
        """memcpy(output_strings->offsets, output_offsets.data(), 4 * (output_strings->count + 1));""",
        s"output_strings->validityBuffer = (unsigned char *) malloc(input_strings->count);",
        s"for( int i = 0; i < input_strings->count; i++ ) {",
        CodeLines.from("set_validity(output_strings->validityBuffer, i, 1);").indented,
        "}"
      )

    val secondOutput = CodeLines.from(
      "// second output",
      s"lengths->data = (int*) malloc(input_strings->count * 4);",
      s"lengths->validityBuffer = (unsigned char *) malloc(input_strings->count);",
      s"lengths->count = input_strings->count;",
      s"for( int i = 0; i < input_strings->count; i++ ) {",
      CodeLines.from(
        "lengths->data[i] = input_strings->offsets[i + 1] - input_strings->offsets[i];",
        "set_validity(lengths->validityBuffer, i, 1);"
      ),
      "}"
    )

    val thirdOutput = CodeLines.from(
      " // third output ",
      """std::string output_result_2("");""",
      """std::vector<int> output_offsets_2;""",
      """int currentOffset_2 = 0;""",
      """for ( int i = 0; i < input_strings->count; i++ ) {""",
      CodeLines.from(
        s"int beginIndex_2 = 1;",
        s"int string_i_length = input_strings->offsets[i + 1] - input_strings->offsets[i];",
        s"int endIndex_2 = string_i_length - 2;",
        """output_offsets_2.push_back(currentOffset_2);""",
    s"""currentOffset_2 += (endIndex_2 - beginIndex_2);""",
    s"for ( int j = beginIndex_2; j < endIndex_2; j++ ) {",
          CodeLines
      .from(
    """output_result_2.append((input_strings->data + (input_strings->offsets[i] + j)), 1);"""
    )
      .indented,
    "}",
    ),
      "}",
      """output_offsets_2.push_back(currentOffset_2);""",
      """output_strings_2->count = input_strings->count;""",
      """output_strings_2->size = currentOffset_2;""",
      """output_strings_2->data = (char*)malloc(output_strings_2->size);""",
      """memcpy(output_strings_2->data, output_result_2.data(), output_strings_2->size);""",
      """output_strings_2->offsets = (int*)malloc(4 * (output_strings_2->count + 1));""",
      """memcpy(output_strings_2->offsets, output_offsets_2.data(), 4 * (output_strings_2->count + 1));""",
      s"output_strings_2->validityBuffer = (unsigned char *) malloc(input_strings->count);",
      s"for( int i = 0; i < input_strings->count; i++ ) {",
      CodeLines.from("set_validity(output_strings_2->validityBuffer, i, 1);").indented,
      "}"
    )

    CodeLines.from(
      """#include <stdio.h>""",
      """#include <stdlib.h>""",
      """#include <string.h>""",
      """#include <iostream>""",
      """#include <string>""",
      """#include "cpp/frovedis/text/words.hpp"""",
      s"""extern "C" long ${fName}(nullable_varchar_vector* input_strings, nullable_varchar_vector* output_strings, nullable_int_vector* lengths, nullable_varchar_vector* output_strings_2) {""",
      CodeLines
        .from(firstOutput, "", secondOutput, "", thirdOutput, "", """return 0;""")
        .indented,
      "}"
    )
  }
}
