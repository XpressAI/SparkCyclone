package com.nec.spark.agile.groupby

import com.nec.cmake.UdpDebug
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CFunction, VeScalarType}
import com.nec.spark.agile.groupby.GroupByOutline.StagedAggregation

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
      body = {
        CodeLines.from(
          UdpDebug.conditional.createSock,
          performGroupingOnKeys,
          computedAggregates.map(Function.tupled(mergeAndProduceAggregatePartialsPerGroup)),
          passProjectionsPerGroup,
          UdpDebug.conditional.close
        )
      }
    )

  def mergeAndProduceAggregatePartialsPerGroup(
    sa: StagedAggregation,
    aggregation: Aggregation
  ): CodeLines =
    CodeLines.from(
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
        afterLast = CodeLines.from(GroupByOutline.storeTo(sa.name, aggregation.fetch(sa.name), "g"))
      )
    )

}
