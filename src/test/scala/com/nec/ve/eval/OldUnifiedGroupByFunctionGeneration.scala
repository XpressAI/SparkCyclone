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
package com.nec.ve.eval

import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{GroupByAggregation, GroupByProjection}
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.core._
import com.nec.spark.agile.StringProducer.CopyStringProducer
import com.nec.spark.agile.groupby.GroupByOutline.{GroupingKey, StagedAggregation, StagedAggregationAttribute, StagedProjection, StringReference}
import com.nec.spark.agile.groupby.{GroupByOutline, GroupByPartialGenerator, GroupByPartialToFinalGenerator, GroupingCodeGenerator}
import com.nec.spark.planning.VERewriteStrategy.SequenceList

final case class OldUnifiedGroupByFunctionGeneration(
  veDataTransformation: VeGroupBy[
    CVector,
    Either[StringGrouping, TypedCExpression2],
    Either[NamedStringProducer, NamedGroupByExpression]
  ]
) {

  def codeGenerator: GroupingCodeGenerator = GroupingCodeGenerator(
    groupingVecName = "full_grouping_vec",
    groupsCountOutName = "groups_count",
    groupsIndicesName = "groups_indices",
    sortedIdxName = "sorted_idx"
  )

  /** Todo clean up the Left/Right thing, it's messy */
  def renderGroupBy: CFunction = {
    val stagedGroupBy = GroupByOutline(
      groupingKeys = veDataTransformation.groups.zipWithIndex.map {
        case (Left(StringGrouping(name)), _) => GroupingKey(name, VeString)
        case (Right(typedExp), i)            => GroupingKey(s"g_$i", typedExp.veType)
      },
      finalOutputs = veDataTransformation.outputs.zipWithIndex.map {
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByAggregation(aggregation))),
              _
            ) =>
          Right(
            StagedAggregation(
              outputName,
              veType,
              aggregation.partialValues(outputName).map { case (cv, _) =>
                StagedAggregationAttribute(name = cv.name, veType)
              }
            )
          )
        case (Right(NamedGroupByExpression(outputName, veType, GroupByProjection(_))), _) =>
          Left(StagedProjection(outputName, veType))
        case (Left(NamedStringProducer(outputName, _)), _) =>
          Left(StagedProjection(outputName, VeString))
      }
    )

    val computeAggregate: StagedAggregation => Either[String, Aggregation] = agg =>
      veDataTransformation.outputs
        .lift(stagedGroupBy.finalOutputs.indexWhere(_.right.exists(_ == agg)))
        .collectFirst { case Right(NamedGroupByExpression(_, _, GroupByAggregation(aggregation))) =>
          aggregation
        }
        .toRight(s"Could not compute aggregate for: ${agg}")

    val pf = {
      for {
        gks <- stagedGroupBy.groupingKeys
          .map(gk =>
            veDataTransformation.groups
              .lift(stagedGroupBy.groupingKeys.indexOf(gk))
              .collect {
                case Left(StringGrouping(name)) => gk -> Left(StringReference(name))
                case Right(v)                   => gk -> Right(v)
              }
              .toRight(s"Could not compute grouping key ${gk}")
          )
          .sequence
        cpr <- stagedGroupBy.projections
          .map(proj =>
            veDataTransformation.outputs
              .lift(stagedGroupBy.finalOutputs.indexWhere(_.left.exists(_ == proj)))
              .collectFirst {
                case Right(NamedGroupByExpression(_, veType, GroupByProjection(cExpression))) =>
                  proj -> Right(TypedCExpression2(veType, cExpression))
                case Left(NamedStringProducer(_, c: CopyStringProducer)) =>
                  proj -> Left(StringReference(c.inputName))
              }
              .toRight(s"Could not compute projection for ${proj}")
          )
          .sequence
        ca <- stagedGroupBy.aggregations.map(sa => computeAggregate(sa).map(c => sa -> c)).sequence
      } yield GroupByPartialGenerator(
        finalGenerator = GroupByPartialToFinalGenerator(stagedGroupBy, ca),
        computedGroupingKeys = gks,
        computedProjections = cpr,
        stringVectorComputations = Nil
      )
        .createPartial(inputs = veDataTransformation.inputs)
    }
      .fold(sys.error, identity)

    val ff = stagedGroupBy.aggregations
      .map(sa => computeAggregate(sa).map(c => sa -> c))
      .sequence
      .map(r => GroupByPartialToFinalGenerator(stagedGroupBy, r).createFinal)
      .fold(sys.error, identity)

    CFunction(
      inputs = pf.inputs,
      outputs = ff.outputs,
      body = CodeLines.from(
        CodeLines.commentHere(
          "Declare the variables for the output of the Partial stage for the unified function"
        ),
        pf.outputs.map(cv => GroupByOutline.declare(cv)),
        pf.body.scoped("Perform the Partial computation stage"),
        ff.body.scoped("Perform the Final computation stage"),
        pf.outputs
          .map(cv => GroupByOutline.dealloc(cv))
          .scoped("Deallocate the partial variables")
      )
    )
  }

}
