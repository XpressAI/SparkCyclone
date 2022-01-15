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

  trait ImperativeStringProducer extends StringProducer {
    def produceTo(tempStringName: String, lenName: String): CodeLines
  }

  trait FrovedisStringProducer extends StringProducer {
    def init(outputName: String, size: String): CodeLines
    def produce(outputName: String, outputIdx: String): CodeLines
    def complete(outputName: String): CodeLines
  }

  def copyString(inputName: String): StringProducer = FrovedisCopyStringProducer(inputName)

  //  def copyString(inputName: String): StringProducer = ImpCopyStringProducer(inputName)

  final case class ImpCopyStringProducer(inputName: String)
    extends ImperativeStringProducer
    with CopyStringProducer {

    override def produceTo(tsn: String, iln: String): CodeLines = {
      CodeLines.from(
        s"std::string sub_str = std::string(${inputName}->data, ${inputName}->offsets[i], ${inputName}->offsets[i+1] - ${inputName}->offsets[i]);",
        s"${tsn}.append(sub_str);",
        s"${iln} += sub_str.size();"
      )
    }
  }

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

    override def init(outputName: String, size: String): CodeLines =
      CodeLines.from(
        s"frovedis::words ${wordName(outputName)} = varchar_vector_to_words(${inputName});",
        s"""std::vector<size_t> ${frovedisStarts(outputName)}(${size});""",
        s"""std::vector<size_t> ${frovedisLens(outputName)}(${size});"""
      )

    override def complete(outputName: String): CodeLines = CodeLines.from(
      s"""std::vector<size_t> ${newStarts(outputName)};""",
      s"""std::vector<int> ${newChars(outputName)} = frovedis::concat_words(
        ${wordName(outputName)}.chars,
        (const vector<size_t>&)(${frovedisStarts(outputName)}),
        (const vector<size_t>&)(${frovedisLens(outputName)}),
        "",
        (vector<size_t>&)(${newStarts(outputName)})
      );""",
      s"""${wordName(outputName)}.chars = ${newChars(outputName)};""",
      s"""${wordName(outputName)}.starts = ${newStarts(outputName)};""",
      s"""${wordName(outputName)}.lens = ${frovedisLens(outputName)};""",
      s"words_to_varchar_vector(${wordName(outputName)}, ${outputName});"
    )

  }

  final case class StringChooser(condition: CExpression, ifTrue: String, otherwise: String)
    extends ImperativeStringProducer {
    override def produceTo(tempStringName: String, lenName: String): CodeLines =
      CodeLines.from(
        // note: does not escape strings
        s"""std::string sub_str = ${condition.cCode} ? std::string("${ifTrue}") : std::string("${otherwise}");""",
        s"${tempStringName}.append(sub_str);",
        s"${lenName} += sub_str.size();"
      )
  }

  final case class FilteringProducer(outputName: String, stringProducer: StringProducer) {
    val tmpString = s"${outputName}_tmp"
    val tmpOffsets = s"${outputName}_tmp_offsets"
    val tmpCurrentOffset = s"${outputName}_tmp_current_offset"
    val tmpCount = s"${outputName}_tmp_count"

    def setup: CodeLines =
      stringProducer match {
        case _: ImperativeStringProducer =>
          CodeLines.from(
            CodeLines.debugHere,
            s"""std::string ${tmpString}("");""",
            s"""std::vector<int32_t> ${tmpOffsets};""",
            s"""int32_t ${tmpCurrentOffset} = 0;""",
            s"int ${tmpCount} = 0;"
          )
        case f: FrovedisStringProducer =>
          CodeLines.from(CodeLines.debugHere, f.init(outputName, "groups_count"))
      }

    def forEach: CodeLines = {
      stringProducer match {
        case imperative: ImperativeStringProducer =>
          CodeLines
            .from(
              CodeLines.debugHere,
              "int len = 0;",
              imperative.produceTo(s"$tmpString", "len"),
              s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
              s"""${tmpCurrentOffset} += len;""",
              s"${tmpCount}++;"
            )
        case frovedisStringProducer: FrovedisStringProducer =>
          CodeLines.from(frovedisStringProducer.produce(outputName, "g"))
      }
    }

    def complete: CodeLines =
      stringProducer match {
        case _: ImperativeStringProducer =>
          CodeLines.from(
            CodeLines.debugHere,
            s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
            s"""${outputName}->count = ${tmpCount};""",
            s"""${outputName}->dataSize = ${tmpCurrentOffset};""",
            s"""${outputName}->data = (char*)malloc(${outputName}->dataSize);""",
            s"""memcpy(${outputName}->data, ${tmpString}.data(), ${outputName}->dataSize);""",
            s"""${outputName}->offsets = (int32_t*)malloc(sizeof(int32_t) * (${outputName}->count + 1));""",
            s"""memcpy(${outputName}->offsets, ${tmpOffsets}.data(), sizeof(int32_t) * (${outputName}->count + 1));""",
            s"${outputName}->validityBuffer = (uint64_t *) malloc(ceil(${outputName}->count / 64.0) * sizeof(uint64_t));",
            CodeLines.debugHere
          )

        case f: FrovedisStringProducer =>
          CodeLines.from(CodeLines.debugHere, f.complete(outputName))
      }

    def validityForEach(idx: String): CodeLines =
      CodeLines.from(s"set_validity($outputName->validityBuffer, $idx, 1);")
  }

  def produceVarChar(
    inputCount: String,
    outputName: String,
    stringProducer: ImperativeStringProducer
  ): CodeLines = {
    val fp = FilteringProducer(outputName, stringProducer)
    CodeLines.from(
      fp.setup,
      s"""for ( int32_t i = 0; i < $inputCount; i++ ) {""",
      fp.forEach.indented,
      "}",
      fp.complete,
      s"for( int32_t i = 0; i < $inputCount; i++ ) {",
      CodeLines.from(fp.validityForEach("i")).indented,
      "}"
    )
  }

  def produceVarChar(
    inputCount: String,
    outputName: String,
    stringProducer: FrovedisStringProducer,
    outputCount: String,
    outputIdx: String
  ): CodeLines = {
    CodeLines.from(
      stringProducer.init(outputName, outputCount),
      s"""for ( int32_t i = 0; i < $inputCount; i++ ) {""",
      stringProducer.produce(outputName, outputIdx).indented,
      "}",
      stringProducer.complete(outputName)
    )
  }
}
