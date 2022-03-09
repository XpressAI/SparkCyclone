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
    val stringVecHashes: List[String] = thingsToGroup.flatMap(_.left.toSeq)

    val elems = thingsToGroup.flatMap {
      case Right(g) =>
        List(g.cCode, g.isNotNullCode.getOrElse("1"))
      case Left(stringName) =>
        List(s"${stringName}_string_hashes[i]")
    }

    // Sort in ASC order for all tuple elements
    val sortOrder = elems.map(_ => 1).mkString(s"std::array<int, ${elems.size}> {{ ", ", ", " }}")

    CodeLines.from(
      // Declare the elements and sorted_indices vectors
      s"std::vector<${tupleType}> ${groupingVecName}(${count});",
      s"std::vector<size_t> ${sortedIdxName}(${count});",
      "",
      // For all string columns, get the hash vector
      stringVecHashes.map { name => s"const auto ${name}_string_hashes = ${name}->hash_vec();" },
      "",
      // Construct the elements vector
      CodeLines.forLoop("i", count) {
        List(
          s"${sortedIdxName}[i] = i;",
          s"${groupingVecName}[i] = ${tupleType}(${elems.mkString(", ")});"
        )
      },
      "",
      // Perform the tuple sort
      s"${sortedIdxName} = cyclone::sort_tuples(${groupingVecName}, ${sortOrder});",
      "",
      // Reconstruct the elements vector using the sorted_indices
      CodeLines.forLoop("j", count) {
        List(
          s"auto i = ${sortedIdxName}[j];",
          s"${groupingVecName}[j] = ${tupleType}(${elems.mkString(", ")});"
        )
      },
      "",
      // Identify the indices where elements first change
      s"std::vector<size_t> ${groupsIndicesName} = frovedis::set_separate(${groupingVecName});",
      s"auto ${groupsCountOutName} = ${groupsIndicesName}.size() - 1;",
      ""
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
          "#pragma cdir nodep",
          "#pragma _NEC ivdep",
          "#pragma _NEC vovertake",
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
