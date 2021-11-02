/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.planning

import com.nec.spark.agile.{CExpressionEvaluation, StringProducer}
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CExpression
import com.nec.spark.agile.StringProducer.ImperativeStringProducer
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
        "nullable_varchar_vector* input_0",
        "nullable_varchar_vector* input_0_o",
        "nullable_varchar_vector* output_strings",
        "nullable_int_vector* lengths",
        "nullable_varchar_vector* output_strings_2"
      ).mkString(", \n"),
      ") {",
      CodeLines
        .from(
          StringProducer
            .produceVarChar(
              "input_0->count",
              "output_strings",
              produce_string_to(beginIndex, endIndex)
            )
            .block,
          select_lengths.block,
          StringProducer
            .produceVarChar("input_0->count", "output_strings_2", produce_substr_dyn)
            .block,
          """return 0;"""
        )
        .indented,
      "}"
    )
  }

  private def produce_substr_dyn: ImperativeStringProducer = (tempStringName, lenName) =>
    CodeLines
      .from(
        s"int32_t beginIndex_2 = 1;",
        s"int32_t string_i_length = input_0->offsets[i + 1] - input_0->offsets[i];",
        s"int32_t endIndex_2 = string_i_length - 2;",
        s"for ( int32_t j = beginIndex_2; j < endIndex_2; j++ ) {",
        CodeLines
          .from(
            s"""${tempStringName}.append((input_0->data + (input_0->offsets[i] + j)), 1);""",
            s"${lenName} += 1;"
          )
          .indented,
        "}"
      )

  private def select_lengths: CodeLines = {
    CodeLines.from(
      "// second output",
      s"lengths->data = (int32_t*) malloc(input_0->count * 4);",
      s"lengths->validityBuffer = (unsigned char *) malloc(input_0->count);",
      s"lengths->count = input_0->count;",
      s"for( int32_t i = 0; i < input_0->count; i++ ) {",
      CodeLines
        .from(
          "lengths->data[i] = input_0->offsets[i + 1] - input_0->offsets[i];",
          "set_validity(lengths->validityBuffer, i, 1);"
        )
        .indented,
      "}"
    )
  }

  private def produce_string_to(beginIndex: Int, endIndex: Int): ImperativeStringProducer =
    (tempStringName, itemLengthName) =>
      CodeLines
        .from(
          s"""for ( int32_t j = ${beginIndex}; j < ${endIndex}; j++ ) {""",
          CodeLines
            .from(
              s"""${tempStringName}.append((input_0->data + (input_0->offsets[i] + j)), 1);""",
              s"${itemLengthName}++;"
            )
            .indented,
          "}",
          s"""for ( int32_t j = input_0_o->offsets[i]; j < input_0_o->offsets[i + 1]; j++ ) {""",
          CodeLines
            .from(s"""${tempStringName}.append(input_0_o->data + j, 1);""", s"${itemLengthName}++;")
            .indented,
          "}",
          "std::string len_str = std::to_string(input_0->offsets[i+1] - input_0->offsets[i]);",
          s"${tempStringName}.append(len_str);",
          s"${itemLengthName} += len_str.size();"
        )

  def expr_to_string(cExpression: CExpression): ImperativeStringProducer =
    (tsn, iln) =>
      CodeLines
        .from(
          s"std::string len_str = std::to_string(${cExpression.cCode});",
          s"${tsn}.append(len_str);",
          s"${iln} += len_str.size();"
        )

}
