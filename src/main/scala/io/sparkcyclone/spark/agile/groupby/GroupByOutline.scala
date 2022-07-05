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
package io.sparkcyclone.spark.agile.groupby

import io.sparkcyclone.spark.agile.CFunctionGeneration._
import io.sparkcyclone.spark.agile.core._
import io.sparkcyclone.spark.agile.groupby.GroupByOutline.{GroupingKey, StagedAggregation, StagedProjection}

/**
 * General class to describe a group-by to create the function outline
 *
 * Its methods are used by [[GroupByPartialGenerator]] and [[GroupByPartialToFinalGenerator]].
 *
 * The reason to have this in a separate space is for a cleaner separation of concerns
 * and providing the very minimum number of dependencies that are really needed.
 */
final case class GroupByOutline(
  groupingKeys: List[GroupingKey],
  finalOutputs: List[Either[StagedProjection, StagedAggregation]]
) {

  def groupingCodeGenerator: GroupingCodeGenerator = GroupingCodeGenerator(
    groupingVecName = "grouping_vec",
    groupsCountOutName = "groups_count",
    groupsIndicesName = "groups_indices",
    sortedIdxName = "sorted_idx"
  )

  def projections: List[StagedProjection] =
    finalOutputs.flatMap(_.left.toSeq)

  def aggregations: List[StagedAggregation] =
    finalOutputs.flatMap(_.right.toSeq)

  def partialOutputs: List[CVector] = {
    List(
      groupingKeys.map(gk =>
        gk.veType match {
          case VeString => gk.veType.makeCVector(s"partial_str_${gk.name}")
          case other    => other.makeCVector(s"partial_${gk.name}")
        }
      ),
      projections.map(pr =>
        pr.veType.makeCVector(
          if (pr.veType.isString) s"partial_str_${pr.name}" else s"partial_${pr.name}"
        )
      ),
      aggregations.flatMap(agg =>
        agg.attributes.map(att => att.veScalarType.makeCVector(s"partial_${att.name}"))
      )
    ).flatten
  }

  def tupleTypes: List[String] =
    groupingKeys
      .flatMap { groupingKey =>
        groupingKey.veType match {
          case vst: VeScalarType => List(vst.cScalarType, "int")
          case VeString          => List("long")
        }
      }

  def tupleType: String =
    tupleTypes.mkString(start = "std::tuple<", sep = ", ", end = ">")

  def performGroupingOnKeys: CodeLines = {
    CodeLines.from(
      groupingCodeGenerator.identifyGroups(
        tupleTypes = tupleTypes,
        tupleType = tupleType,
        count = s"${partialOutputs.head.name}->count",
        thingsToGroup = groupingKeys.map(gk =>
          gk.veType match {
            case _: VeScalarType =>
              Right(
                CExpression(
                  s"partial_${gk.name}->data[i]",
                  Some(s"partial_${gk.name}->get_validity(i)")
                )
              )
            case VeString => Left(s"partial_str_${gk.name}")
          }
        )
      ),
      "",
      s"std::vector<size_t> matching_ids(${groupingCodeGenerator.groupsCountOutName});",
      s"size_t* matching_ids_arr = matching_ids.data();",
      "#pragma _NEC vector",
      CodeLines.forLoop("g", groupingCodeGenerator.groupsCountOutName) {
        s"matching_ids_arr[g] = ${groupingCodeGenerator.sortedIdxName}[${groupingCodeGenerator.groupsIndicesName}[g]];"
      },
      ""
    )
  }

  def passProjectionsPerGroup: CodeLines =
    CodeLines.from(projections.map {
      case StagedProjection(name, VeString) =>
        CodeLines.from(s"${name}->move_assign_from(partial_str_${name}->select(matching_ids));")
      case stagedProjection @ StagedProjection(_, scalarType: VeScalarType) =>
        CodeLines.from(
          GroupByOutline.initializeScalarVector(
            veScalarType = scalarType,
            variableName = stagedProjection.name,
            countExpression = groupingCodeGenerator.groupsCountOutName
          ),
          groupingCodeGenerator.forHeadOfEachGroup(
            CodeLines.from(
              GroupByOutline.storeTo(
                stagedProjection.name,
                CExpression(
                  cCode = s"partial_${stagedProjection.name}->data[i]",
                  isNotNullCode = Some(s"partial_${stagedProjection.name}->get_validity(i)")
                ),
                "g"
              )
            )
          )
        )
    })

}

object GroupByOutline {
  def initializeStringVector(variableName: String): CodeLines = CodeLines.empty

  def debugVector(name: String): CodeLines = {
    s"${name}->print();"
  }

  def dealloc(cv: CVector): CodeLines = CodeLines.empty

  def declare(cv: CVector): CodeLines =
    CodeLines.from(s"${cv.veType.cVectorType} *${cv.name} = ${cv.veType.cVectorType}::allocate();")

  final case class StringReference(name: String)
  final case class InputReference(name: String)
  final case class GroupingKey(name: String, veType: VeType)
  final case class StagedProjection(name: String, veType: VeType)
  final case class StagedAggregationAttribute(name: String, veScalarType: VeScalarType)
  final case class StagedAggregation(
    name: String,
    finalType: VeType,
    attributes: List[StagedAggregationAttribute]
  )

  def storeTo(outname: String, expr: CExpression, idx: String): CodeLines = {
    // Flatten out the if-clause to make the loop more vectorizable
    val condition = expr.isNotNullCode.getOrElse("1")
    CodeLines.from(
      s"""${outname}->data[${idx}] = ${expr.cCode};""",
      s"${outname}->set_validity(${idx}, ${condition});"
    )
  }

  def initializeScalarVector(
    veScalarType: VeScalarType,
    variableName: String,
    countExpression: String
  ): CodeLines = {
    s"${variableName}->resize(${countExpression});"
  }

  def scalarVectorFromStdVector(
    veScalarType: VeScalarType,
    targetName: String,
    sourceName: String
  ): CodeLines =
    CodeLines.from(
      s"$targetName = ${veScalarType.cVectorType}::allocate();",
      initializeScalarVector(veScalarType, targetName, s"$sourceName.size()"),
      s"for ( int x = 0; x < $sourceName.size(); x++ ) {",
      CodeLines
        .from(s"$targetName->data[x] = $sourceName[x];", s"$targetName->set_validity(x, 1);")
        .indented,
      "}"
    )

}
