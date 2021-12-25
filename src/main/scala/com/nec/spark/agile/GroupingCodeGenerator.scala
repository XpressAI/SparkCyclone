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

final case class GroupingCodeGenerator(
  groupingVecName: String,
  groupsCountOutName: String,
  groupsIndicesName: String,
  sortedIdxName: String
) {

  def identifyGroups(
    tupleTypes: List[String],
    tupleType: String,
    count: String,
    thingsToGroup: List[Either[String, CExpression]]
  ): CodeLines = {
    val stringsToHash: List[String] = thingsToGroup.flatMap(_.left.toSeq)
    CodeLines.from(
      s"std::vector<${tupleType}> ${groupingVecName}(${count});",
      s"std::vector<size_t> ${sortedIdxName}(${count});",
      stringsToHash.map { name =>
        val stringIdToHash = s"${name}_string_id_to_hash"
        val stringHashTmp = s"${name}_string_id_to_hash_tmp"
        CodeLines.from(
          s"std::vector<long> $stringIdToHash(${count});",
          s"for ( long i = 0; i < ${count}; i++ ) {",
          CodeLines
            .from(
              s"long ${stringHashTmp} = 0;",
              s"for ( int q = ${name}->offsets[i]; q < ${name}->lengths[i]; q++ ) {",
              CodeLines
                .from(s"${stringHashTmp} = 31*${stringHashTmp} + ${name}->data[q];")
                .indented,
              "}",
              s"$stringIdToHash[i] = ${stringHashTmp};"
            )
            .indented,
          "}"
        )
      },
      CodeLines.debugHere,
      s"for ( long i = 0; i < ${count}; i++ ) {",
      CodeLines
        .from(
          s"${sortedIdxName}[i] = i;",
          s"${groupingVecName}[i] = ${tupleType}(${thingsToGroup
            .flatMap {
              case Right(g) => List(g.cCode, g.isNotNullCode.getOrElse("1"))
              case Left(stringName) =>
                List(s"${stringName}_string_id_to_hash[i]")
            }
            .mkString(", ")});"
        )
        .indented,
      s"}",
      CodeLines.debugHere,
      tupleTypes.zipWithIndex.reverse.collect { case (t, idx) =>
        CodeLines.from(
          s"{",
          s"std::vector<${t}> temp(${count});",
          s"for ( long i = 0; i < ${count}; i++ ) {",
          CodeLines
            .from(s"temp[i] = std::get<${idx}>(${groupingVecName}[${sortedIdxName}[i]]);")
            .indented,
          s"}",
          s"frovedis::radix_sort(temp.data(), ${sortedIdxName}.data(), temp.size());",
          s"}"
        )
      },
      s"for ( long j = 0; j < ${count}; j++ ) {",
      CodeLines
        .from(
          s"long i = ${sortedIdxName}[j];",
          s"${groupingVecName}[j] = ${tupleType}(${thingsToGroup
            .flatMap {
              case Right(g) => List(g.cCode, g.isNotNullCode.getOrElse("1"))
              case Left(stringName) =>
                List(s"${stringName}_string_id_to_hash[i]")
            }
            .mkString(", ")});"
        )
        .indented,
      s"}",
      CodeLines.debugHere,
      s"std::vector<size_t> ${groupsIndicesName} = frovedis::set_separate(${groupingVecName});",
      s"int ${groupsCountOutName} = ${groupsIndicesName}.size() - 1;"
    )
  }

  def forHeadOfEachGroup(f: => CodeLines): CodeLines =
    CodeLines
      .from(
        s"for (size_t g = 0; g < ${groupsCountOutName}; g++) {",
        CodeLines
          .from(s"long i = ${sortedIdxName}[${groupsIndicesName}[g]];", f)
          .indented,
        "}"
      )

  def forEachGroupItem(
    beforeFirst: => CodeLines,
    perItem: => CodeLines,
    afterLast: => CodeLines
  ): CodeLines =
    CodeLines.from(
      s"for (size_t g = 0; g < ${groupsCountOutName}; g++) {",
      CodeLines
        .from(
          s"size_t group_start_in_idx = ${groupsIndicesName}[g];",
          s"size_t group_end_in_idx = ${groupsIndicesName}[g + 1];",
          "int i = 0;",
          beforeFirst,
          s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
          CodeLines
            .from(s"i = ${sortedIdxName}[j];", perItem)
            .indented,
          "}",
          afterLast
        )
        .indented,
      "}"
    )
}
