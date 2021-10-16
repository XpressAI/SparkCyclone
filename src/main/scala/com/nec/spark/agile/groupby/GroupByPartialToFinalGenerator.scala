package com.nec.spark.agile.groupby

import com.nec.cmake.UdpDebug
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CFunction, VeScalarType}
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
        UdpDebug.conditional.createSock,
        CodeLines
          .from(
            performGroupingOnKeys,
            computedAggregates.map(Function.tupled(mergeAndProduceAggregatePartialsPerGroup)),
            passProjectionsPerGroup
          )
          .time("Execution of Final"),
        UdpDebug.conditional.close
      )
    )

  def mergeAndProduceAggregatePartialsPerGroup(
    sa: StagedAggregation,
    aggregation: Aggregation
  ): CodeLines =
    CodeLines
      .from(
        CodeLines.debugHere,
        GroupByOutline.initializeScalarVector(
          veScalarType = sa.finalType.asInstanceOf[VeScalarType],
          variableName = sa.name,
          countExpression = groupingCodeGenerator.groupsCountOutName
        ),
        CodeLines.commentHere("producing aggregate/partials per group"),
        groupingCodeGenerator.forEachGroupItem(
          beforeFirst = aggregation.initial(sa.name),
          perItem = aggregation.merge(sa.name, s"partial_${sa.name}"),
          afterLast =
            CodeLines.from(GroupByOutline.storeTo(sa.name, aggregation.fetch(sa.name), "g"))
        )
      )
      .time(s"Staged Aggregation ${sa.name}")

}
