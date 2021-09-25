package com.nec.spark.planning

import com.nec.spark.agile.{CExpressionEvaluation, StringProducer}
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.apache.spark.sql.catalyst.expressions.Attribute

//noinspection SameParameterValue
object StringCExpressionEvaluation {
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
        .from(
          StringProducer
            .produceVarChar("output_strings", produce_string_to(beginIndex, endIndex))
            .block,
          select_lengths.block,
          StringProducer.produceVarChar("output_strings_2", produce_substr_dyn).block,
          """return 0;"""
        )
        .indented,
      "}"
    )
  }

  private def produce_substr_dyn: StringProducer = (tempStringName, lenName) =>
    CodeLines
      .from(
        s"int32_t beginIndex_2 = 1;",
        s"int32_t string_i_length = input_strings->offsets[i + 1] - input_strings->offsets[i];",
        s"int32_t endIndex_2 = string_i_length - 2;",
        s"for ( int32_t j = beginIndex_2; j < endIndex_2; j++ ) {",
        CodeLines
          .from(
            s"""${tempStringName}.append((input_strings->data + (input_strings->offsets[i] + j)), 1);""",
            s"${lenName} += 1;"
          )
          .indented,
        "}"
      )

  private def select_lengths: CodeLines = {
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

  private def produce_string_to(beginIndex: Int, endIndex: Int): StringProducer =
    (tempStringName, itemLengthName) => {
      CodeLines
        .from(
          s"""for ( int32_t j = ${beginIndex}; j < ${endIndex}; j++ ) {""",
          CodeLines
            .from(
              s"""${tempStringName}.append((input_strings->data + (input_strings->offsets[i] + j)), 1);""",
              s"${itemLengthName}++;"
            )
            .indented,
          "}",
          s"""for ( int32_t j = input_strings_o->offsets[i]; j < input_strings_o->offsets[i + 1]; j++ ) {""",
          CodeLines
            .from(
              s"""${tempStringName}.append(input_strings_o->data + j, 1);""",
              s"${itemLengthName}++;"
            )
            .indented,
          "}",
          "std::string len_str = std::to_string(input_strings->offsets[i+1] - input_strings->offsets[i]);",
          s"${tempStringName}.append(len_str);",
          s"${itemLengthName} += len_str.size();"
        )

    }

}
