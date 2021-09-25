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

    val firstOutput: CodeLines = select_substring(beginIndex, endIndex)

    val secondOutput = select_lengths

    val thirdOutput = select_complex_substring

    CodeLines.from(
      """#include <stdio.h>""",
      """#include <stdlib.h>""",
      """#include <string.h>""",
      """#include <iostream>""",
      """#include <string>""",
      """#include "cpp/frovedis/text/words.hpp"""",
      s"""extern "C" long ${fName}(""",
      List(
        "nullable_varchar_vector* input_strings",
        "nullable_varchar_vector* input_strings_o",
        "nullable_varchar_vector* output_strings",
        "nullable_int_vector* lengths",
        "nullable_varchar_vector* output_strings_2"
      ).mkString(", \n"),
      ") {",
      CodeLines
        .from(firstOutput, "", secondOutput, "", thirdOutput, "", """return 0;""")
        .indented,
      "}"
    )
  }

  private def select_complex_substring = {
    CodeLines.from(
      " // third output ",
      """std::string output_result_2("");""",
      """std::vector<int32_t> output_offsets_2;""",
      """int32_t currentOffset_2 = 0;""",
      """for ( int32_t i = 0; i < input_strings->count; i++ ) {""",
      CodeLines
        .from(
          s"int32_t beginIndex_2 = 1;",
          s"int32_t string_i_length = input_strings->offsets[i + 1] - input_strings->offsets[i];",
          s"int32_t endIndex_2 = string_i_length - 2;",
          """output_offsets_2.push_back(currentOffset_2);""",
          s"""currentOffset_2 += (endIndex_2 - beginIndex_2);""",
          s"for ( int32_t j = beginIndex_2; j < endIndex_2; j++ ) {",
          CodeLines
            .from(
              """output_result_2.append((input_strings->data + (input_strings->offsets[i] + j)), 1);"""
            )
            .indented,
          "}"
        )
        .indented,
      "}",
      """output_offsets_2.push_back(currentOffset_2);""",
      """output_strings_2->count = input_strings->count;""",
      """output_strings_2->size = currentOffset_2;""",
      """output_strings_2->data = (char*)malloc(output_strings_2->size);""",
      """memcpy(output_strings_2->data, output_result_2.data(), output_strings_2->size);""",
      """output_strings_2->offsets = (int32_t*)malloc(4 * (output_strings_2->count + 1));""",
      """memcpy(output_strings_2->offsets, output_offsets_2.data(), 4 * (output_strings_2->count + 1));""",
      s"output_strings_2->validityBuffer = (unsigned char *) malloc(input_strings->count);",
      s"for( int32_t i = 0; i < input_strings->count; i++ ) {",
      CodeLines.from("set_validity(output_strings_2->validityBuffer, i, 1);").indented,
      "}"
    )
  }

  private def select_lengths = {
    CodeLines.from(
      "// second output",
      s"lengths->data = (int32_t*) malloc(input_strings->count * 4);",
      s"lengths->validityBuffer = (unsigned char *) malloc(input_strings->count);",
      s"lengths->count = input_strings->count;",
      s"for( int32_t i = 0; i < input_strings->count; i++ ) {",
      CodeLines
        .from(
          "lengths->data[i] = input_strings->offsets[i + 1] - input_strings->offsets[i];",
          "set_validity(lengths->validityBuffer, i, 1);"
        )
        .indented,
      "}"
    )
  }

  private def select_substring(beginIndex: Int, endIndex: Int) = {
    val firstOutput = CodeLines
      .from(
        " // first output ",
        """std::string output_result("");""",
        """std::vector<int32_t> output_offsets;""",
        """int32_t currentOffset = 0;""",
        """for ( int32_t i = 0; i < input_strings->count; i++ ) {""",
        CodeLines
          .from(
            "int length = 0;",
            s"""for ( int32_t j = ${beginIndex}; j < ${endIndex}; j++ ) {""",
            CodeLines
              .from(
                """output_result.append((input_strings->data + (input_strings->offsets[i] + j)), 1);""",
                "length++;"
              )
              .indented,
            "}",
            """output_offsets.push_back(currentOffset);""",
            s"""for ( int32_t j = input_strings_o->offsets[i]; j < input_strings_o->offsets[i + 1]; j++ ) {""",
            CodeLines
              .from(
                """output_result.append(input_strings_o->data + j, 1);""",
                "length++;"
              )
              .indented,
            "}",
            s"""currentOffset += length;"""
          )
          .indented,
        """}""",
        """output_offsets.push_back(currentOffset);""",
        """output_strings->count = input_strings->count;""",
        """output_strings->size = currentOffset;""",
        """output_strings->data = (char*)malloc(output_strings->size);""",
        """memcpy(output_strings->data, output_result.data(), output_strings->size);""",
        """output_strings->offsets = (int32_t*)malloc(4 * (output_strings->count + 1));""",
        """memcpy(output_strings->offsets, output_offsets.data(), 4 * (output_strings->count + 1));""",
        s"output_strings->validityBuffer = (unsigned char *) malloc(input_strings->count);",
        s"for( int32_t i = 0; i < input_strings->count; i++ ) {",
        CodeLines.from("set_validity(output_strings->validityBuffer, i, 1);").indented,
        "}"
      )
    firstOutput
  }
}
