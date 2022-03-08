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
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CFunction, CVector, TypedCExpression2}
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.groupby.GroupByOutline._

final case class GroupByPartialGenerator(
  finalGenerator: GroupByPartialToFinalGenerator,
  computedGroupingKeys: List[(GroupingKey, Either[StringReference, TypedCExpression2])],
  computedProjections: List[(StagedProjection, Either[StringReference, TypedCExpression2])],
  stringVectorComputations: List[StringHoleEvaluation]
) {
  import finalGenerator._
  import stagedGroupBy._

  def createFull(inputs: List[CVector]): CFunction = {
    val finalFunction = finalGenerator.createFinal
    val partialFunction = createPartial(inputs)
    CFunction(
      inputs = partialFunction.inputs,
      outputs = finalFunction.outputs,
      body = CodeLines.from(
        CodeLines.commentHere(
          "Declare the variables for the output of the Partial stage for the unified function"
        ),
        partialFunction.outputs.map(cv => GroupByOutline.declare(cv)),
        partialFunction.body.blockCommented("Perform the Partial computation stage"),
        finalFunction.body.blockCommented("Perform the Final computation stage"),
        partialFunction.outputs
          .map(cv => GroupByOutline.dealloc(cv))
          .blockCommented("Deallocate the partial variables")
      )
    )
  }

  def createPartial(inputs: List[CVector]): CFunction =
    CFunction(
      inputs = inputs,
      outputs = partialOutputs,
      body = CodeLines.from(
        performGrouping(count = s"${inputs.head.name}->count"),
        stringVectorComputations.map(_.computeVector),
        computeGroupingKeysPerGroup,
        computedProjections.map { case (sp, e) =>
          computeProjectionsPerGroup(sp, e)
        },
        computedAggregates.map { case (a, ag) =>
          computeAggregatePartialsPerGroup(a, ag)
        }
      )
    )

  def computeProjectionsPerGroup(
    stagedProjection: StagedProjection,
    r: Either[StringReference, TypedCExpression2]
  ): CodeLines = r match {
    case Left(StringReference(sr)) =>
      CodeLines.from(
        s"partial_str_${stagedProjection.name}->move_assign_from(${sr}->select(matching_ids));"
      )
    case Right(TypedCExpression2(veType, cExpression)) =>
      CodeLines.from(
        GroupByOutline.initializeScalarVector(
          veType,
          s"partial_${stagedProjection.name}",
          groupingCodeGenerator.groupsCountOutName
        ),
        groupingCodeGenerator.forHeadOfEachGroup(
          GroupByOutline.storeTo(s"partial_${stagedProjection.name}", cExpression, "g")
        )
      )
  }

  def computeAggregatePartialsPerGroup(
    stagedAggregation: StagedAggregation,
    aggregate: Aggregation
  ): CodeLines = {
    val prefix = s"partial_${stagedAggregation.name}"
    CodeLines.from(
      stagedAggregation.attributes.map { attribute =>
        GroupByOutline.initializeScalarVector(
          veScalarType = attribute.veScalarType,
          variableName = s"partial_${attribute.name}",
          countExpression = groupingCodeGenerator.groupsCountOutName
        )
      },
      groupingCodeGenerator.forEachGroupItem(
        beforeFirst = aggregate.initial(prefix),
        perItem = aggregate.iterate(prefix),
        afterLast =
          CodeLines.from(stagedAggregation.attributes.zip(aggregate.partialValues(prefix)).map {
            case (attr, (_, ex)) =>
              CodeLines.from(GroupByOutline.storeTo(s"partial_${attr.name}", ex, "g"))
          })
      )
    )
  }

  def performGrouping(count: String): CodeLines = {
    CodeLines.from(
      groupingCodeGenerator.identifyGroups(
        tupleTypes = tupleTypes,
        tupleType = tupleType,
        count = count,
        thingsToGroup = computedGroupingKeys.map { case (_, e) =>
          e.map(_.cExpression).left.map(_.name)
        }
      ),
      "",
      s"std::vector<size_t> matching_ids(${groupingCodeGenerator.groupsCountOutName});",
      CodeLines.forLoop("g", groupingCodeGenerator.groupsCountOutName) {
        s"matching_ids[g] = ${groupingCodeGenerator.sortedIdxName}[${groupingCodeGenerator.groupsIndicesName}[g]];"
      },
      ""
    )
  }

  def computeGroupingKeysPerGroup: CodeLines = {
    final case class ProductionTriplet(init: CodeLines, forEach: CodeLines, complete: CodeLines)
    val initVars = computedGroupingKeys.map {
      case (groupingKey, Right(TypedCExpression2(scalarType, cExp))) =>
        ProductionTriplet(
          init = GroupByOutline.initializeScalarVector(
            veScalarType = scalarType,
            variableName = s"partial_${groupingKey.name}",
            countExpression = groupingCodeGenerator.groupsCountOutName
          ),
          forEach = storeTo(s"partial_${groupingKey.name}", cExp, "g"),
          complete = CodeLines.empty
        )
      case (groupingKey, Left(StringReference(sr))) =>
        ProductionTriplet(
          init = CodeLines.empty,
          forEach = CodeLines.empty,
          complete = CodeLines.from(
            s"partial_str_${groupingKey.name}->move_assign_from(${sr}->select(matching_ids));"
          )
        )
    }

    CodeLines.scoped("Compute grouping keys per group") {
      CodeLines.from(
        initVars.map(_.init),
        groupingCodeGenerator.forHeadOfEachGroup(initVars.map(_.forEach)),
        initVars.map(_.complete)
      )
    }
  }
}
