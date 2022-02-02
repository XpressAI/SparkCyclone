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
package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CExpression

sealed trait StringProducer extends Serializable {}

object StringProducer {

  trait FrovedisStringProducer extends StringProducer {
    def init(outputName: String, size: String, capacity: String): CodeLines
    def produce(outputName: String, outputIdx: String): CodeLines
    def complete(outputName: String): CodeLines
    def copyValidityBuffer(outputName: String, subsetIndexes: Option[String]): CodeLines
  }

  def copyString(inputName: String): StringProducer = FrovedisCopyStringProducer(inputName)

  sealed trait CopyStringProducer {
    def inputName: String
  }

  final case class FrovedisCopyStringProducer(inputName: String)
    extends FrovedisStringProducer
    with CopyStringProducer {

    def frovedisStarts(outputName: String) = s"${outputName}_starts"
    def frovedisLens(outputName: String) = s"${outputName}_lens"

    def wordName(outputName: String) = s"${outputName}_input_words"
    def newChars(outputName: String) = s"${outputName}_new_chars"
    def newStarts(outputName: String) = s"${outputName}_new_starts"

    def produce(outputName: String, outputIdx: String): CodeLines =
      CodeLines.from(
        s"${frovedisStarts(outputName)}[$outputIdx] = ${wordName(outputName)}.starts[i];",
        s"${frovedisLens(outputName)}[$outputIdx] = ${wordName(outputName)}.lens[i];"
      )

    override def init(outputName: String, size: String, capacity: String): CodeLines =
      CodeLines.from(
        s"frovedis::words ${wordName(outputName)} = varchar_vector_to_words(${inputName});",
        s"""std::vector<size_t> ${frovedisStarts(outputName)}(${size});""",
        s"""std::vector<size_t> ${frovedisLens(outputName)}(${size});""",
        s"${frovedisStarts(outputName)}.reserve(${capacity});",
        s"${frovedisLens(outputName)}.reserve(${capacity});"
      )

    override def complete(outputName: String): CodeLines = CodeLines.from(
      s"""std::vector<size_t> ${newStarts(outputName)};""",
      s"""std::vector<int> ${newChars(outputName)} = frovedis::concat_words(
        ${wordName(outputName)}.chars,
        (const std::vector<size_t>&)(${frovedisStarts(outputName)}),
        (const std::vector<size_t>&)(${frovedisLens(outputName)}),
        "",
        (std::vector<size_t>&)(${newStarts(outputName)})
      );""",
      s"""${wordName(outputName)}.chars = ${newChars(outputName)};""",
      s"""${wordName(outputName)}.starts = ${newStarts(outputName)};""",
      s"""${wordName(outputName)}.lens = ${frovedisLens(outputName)};""",
      s"words_to_varchar_vector(${wordName(outputName)}, ${outputName});"
    )

    override def copyValidityBuffer(
      outputName: String,
      subsetIndexes: Option[String]
    ): CodeLines = {
      subsetIndexes match {
        case Some(subset) =>
          CodeLines.forLoop("g", s"${subset}.size()") {
            List(
              s"int i = ${subset}[g];",
              s"set_validity(${outputName}->validityBuffer, g, check_valid(${inputName}->validityBuffer, i));"
            )
          }

        case None =>
          CodeLines.forLoop("i", s"${outputName}->count") {
            s"set_validity(${outputName}->validityBuffer, i, check_valid(${inputName}->validityBuffer, i));"
          }
      }
    }
  }

  final case class StringChooser(condition: CExpression, ifTrue: String, otherwise: String)
    extends FrovedisStringProducer {
    def init(outputName: String, size: String, capacity: String): CodeLines = {
      CodeLines.from(
        s"""std::vector<int> ${outputName}_chars = frovedis::char_to_int("${ifTrue}a");""",
        s"""int ${outputName}_if_true_pos = 0;""",
        s"""int ${outputName}_if_true_len = ${outputName}_chars.size();""",
        s"""int ${outputName}_otherwise_pos = ${outputName}_chars.size();""",
        s"""std::vector<int> ${outputName}_otherwise = frovedis::char_to_int("${otherwise}");""",
        s"""int ${outputName}_otherwise_len = ${outputName}_chars.size();""",
        s"""${outputName}_chars.insert(${outputName}_chars.end(), ${outputName}_otherwise.begin(), ${outputName}_otherwise.end());""",
        s"""std::vector<size_t> ${outputName}_starts();""", // the length of this should be known...
        s"""std::vector<size_t> ${outputName}_lens();""" // same here.
      )
    }
    def produce(outputName: String, outputIdx: String): CodeLines = {
      CodeLines.from(
        s"if (${condition.cCode}) {",
        CodeLines.from(
          s"${outputName}_starts[i] = ${outputName}_if_true_pos;",
          s"${outputName}_lens[i] = ${outputName}_if_true_len;"
        ),
        "} else {",
        CodeLines.from(
          s"${outputName}_starts[i] = ${outputName}_otherwise_pos;",
          s"${outputName}_lens[i] = ${outputName}_otherwise_len;"
        ),
        "}"
      )
    }
    def complete(outputName: String): CodeLines = {
      CodeLines.from(
        s"frovedis::words ${outputName}_words;",
        s"${outputName}_words.chars.swap(${outputName}_chars);",
        s"${outputName}_words.starts.swap(${outputName}_starts);",
        s"${outputName}_words.lens.swap(${outputName}_lens);"
      )
    }

    def copyValidityBuffer(outputName: String, subsetIndexes: Option[String]): CodeLines = {
      CodeLines.empty
    }
  }

  final case class FilteringProducer(outputName: String, stringProducer: StringProducer) {
    val tmpString = s"${outputName}_tmp"
    val tmpOffsets = s"${outputName}_tmp_offsets"
    val tmpCurrentOffset = s"${outputName}_tmp_current_offset"
    val tmpCount = s"${outputName}_tmp_count"

    def setup(size: String = "groups_count", capacity: String = "0"): CodeLines =
      stringProducer match {
        case f: FrovedisStringProducer =>
          CodeLines.from(CodeLines.debugHere, f.init(outputName, size, capacity))
      }

    def forEach(outputIdx: String): CodeLines = {
      stringProducer match {
        case frovedisStringProducer: FrovedisStringProducer =>
          CodeLines.from(frovedisStringProducer.produce(outputName, outputIdx))
      }
    }

    def complete: CodeLines =
      stringProducer match {
        case f: FrovedisStringProducer =>
          CodeLines.from(CodeLines.debugHere, f.complete(outputName))
      }

    def validityForEach(idx: String): CodeLines =
      CodeLines.from(s"set_validity($outputName->validityBuffer, $idx, 1);")
  }

  def produceVarChar(
    inputCount: String,
    outputName: String,
    stringProducer: FrovedisStringProducer,
    outputCount: String,
    outputIdx: String
  ): CodeLines = {
    CodeLines.from(
      stringProducer.init(outputName, outputCount, "0"),
      CodeLines.forLoop("i", inputCount) {
        stringProducer.produce(outputName, outputIdx)
      },
      stringProducer.complete(outputName),
      stringProducer.copyValidityBuffer(outputName, None)
    )
  }
}
