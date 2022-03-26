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

import com.nec.spark.agile.core.{CodeLines, VeScalarType}
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CFunction}
import com.nec.spark.agile.groupby.GroupByOutline.StagedAggregation

/**
 * In a staged group by, the final stage only needs the information on Aggregations,
 * so the number of parameters here is quite limited. Everything else is
 * passing on of projections because they had already been computed.
 */
final case class GroupByPartialToFinalGenerator(
  stagedGroupBy: GroupByOutline,
  computedAggregates: List[(StagedAggregation, Aggregation)]
) {
  import stagedGroupBy._

  def createFinal: CFunction =
    CFunction(
      inputs = partialOutputs,
      outputs = finalOutputs.map {
        case Left(stagedProjection) => stagedProjection.veType.makeCVector(stagedProjection.name)
        case Right(stagedAggregation) =>
          stagedAggregation.finalType.makeCVector(stagedAggregation.name)
      },
      body = CodeLines.from(
        performGroupingOnKeys,
        computedAggregates.map(Function.tupled(mergeAndProduceAggregatePartialsPerGroup)),
        passProjectionsPerGroup
      )
    )

  def mergeAndProduceAggregatePartialsPerGroup(
    sa: StagedAggregation,
    aggregation: Aggregation
  ): CodeLines =
    CodeLines.from(
      GroupByOutline.initializeScalarVector(
        veScalarType = sa.finalType.asInstanceOf[VeScalarType],
        variableName = sa.name,
        countExpression = groupingCodeGenerator.groupsCountOutName
      ),
      CodeLines.commentHere("producing aggregate/partials per group"),
      groupingCodeGenerator.forEachGroupItem(
        beforeFirst = aggregation.initial(sa.name),
        perItem = aggregation.merge(sa.name, s"partial_${sa.name}"),
        afterLast = CodeLines.from(GroupByOutline.storeTo(sa.name, aggregation.fetch(sa.name), "g"))
      )
    )
}
