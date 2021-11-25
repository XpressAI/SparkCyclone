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
package com.nec.spark.agile.groupby

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.StringProducer.FilteringProducer
import com.nec.spark.agile.groupby.GroupByOutline.{
  GroupingKey,
  StagedAggregation,
  StagedProjection,
  StringReference
}
import com.nec.spark.agile.{GroupingCodeGenerator, StringProducer}

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

  def performGroupingOnKeys: CodeLines =
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
                  Some(s"check_valid(partial_${gk.name}->validityBuffer, i)")
                )
              )
            case VeString => Left(s"partial_str_${gk.name}")
          }
        )
      )
    )

  def passProjectionsPerGroup: CodeLines =
    CodeLines.from(projections.map {
      case StagedProjection(name, VeString) =>
        val fp = FilteringProducer(name, StringProducer.copyString(s"partial_str_${name}"))
        CodeLines
          .from(
            CodeLines.debugHere,
            fp.setup,
            groupingCodeGenerator.forHeadOfEachGroup(fp.forEach),
            fp.complete,
            groupingCodeGenerator.forHeadOfEachGroup(fp.validityForEach("g"))
          )
          .block
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
                  isNotNullCode =
                    Some(s"check_valid(partial_${stagedProjection.name}->validityBuffer, i)")
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
    CodeLines.from(
      s"for (int i = 0; i < $name->count; i++) {",
      CodeLines.from(
        s"""std::cout << "${name}[" << i << "] = " << ${name}->data[i] << " (valid? " << check_valid(${name}->validityBuffer, i) << ")" << std::endl << std::flush; """
      ),
      "}"
    )
  }

  def dealloc(cv: CVector): CodeLines = CodeLines.empty

  def declare(cv: CVector): CodeLines = CodeLines.from(
    s"""${cv.veType.cVectorType} *${cv.name} = (${cv.veType.cVectorType}*)allocate(1, sizeof(${cv.veType.cVectorType}), "declare::${cv.name}");"""
  )

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

  def storeTo(outputName: String, cExpression: CExpression, idx: String): CodeLines =
    cExpression.isNotNullCode match {
      case None =>
        CodeLines.from(
          s"""$outputName->data[${idx}] = ${cExpression.cCode};""",
          s"set_validity($outputName->validityBuffer, ${idx}, 1);"
        )
      case Some(notNullCheck) =>
        CodeLines.from(
          s"if ( $notNullCheck ) {",
          CodeLines
            .from(
              s"""$outputName->data[${idx}] = ${cExpression.cCode};""",
              s"set_validity($outputName->validityBuffer, ${idx}, 1);"
            )
            .indented,
          "} else {",
          CodeLines.from(s"set_validity($outputName->validityBuffer, ${idx}, 0);").indented,
          "}"
        )
    }

  def initializeScalarVector(
    veScalarType: VeScalarType,
    variableName: String,
    countExpression: String
  ): CodeLines =
    CodeLines.from(
      s"$variableName->count = ${countExpression};",
      s"""$variableName->data = (${veScalarType.cScalarType}*) allocate($variableName->count, sizeof(${veScalarType.cScalarType}), "gbo::$variableName->data");""",
      s"""$variableName->validityBuffer = (uint64_t *) allocate(ceil(${countExpression} / 64.0), sizeof(uint64_t), "gbo::$variableName->validityBuffer");"""
    )

}
