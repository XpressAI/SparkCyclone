package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeType}
import com.nec.spark.agile.StagedGroupBy.{GroupingKey, StagedAggregation, StagedProjection}

final case class StagedGroupBy(
  groupingKeys: List[GroupingKey],
  finalOutputs: List[Either[GroupingKey, Either[StagedProjection, StagedAggregation]]]
) {

  def intermediateTypes: List[VeType] =
    groupingKeys
      .map(_.veType) ++ finalOutputs
      .flatMap(_.right.toSeq)
      .flatMap(_.left.toSeq)
      .map(_.veType) ++ finalOutputs
      .flatMap(_.right.toSeq)
      .flatMap(_.right.toSeq)
      .flatMap(_.attributes.map(_.veType))

  def outputs: List[CVector] = finalOutputs.map {
    case Left(groupingKey)             => groupingKey.veType.makeCVector(groupingKey.name)
    case Right(Left(stagedProjection)) => stagedProjection.veType.makeCVector(stagedProjection.name)
    case Right(Right(stagedAggregation)) =>
      stagedAggregation.finalType.makeCVector(stagedAggregation.name)
  }

  def projections: List[StagedProjection] =
    finalOutputs.flatMap(_.right.toSeq).flatMap(_.left.toSeq)

  def aggregations: List[StagedAggregation] =
    finalOutputs.flatMap(_.right.toSeq).flatMap(_.right.toSeq)

  private def partials: List[CVector] = {
    List(
      groupingKeys.map(gk => gk.veType.makeCVector(gk.name)),
      projections.map(pr => pr.veType.makeCVector(pr.name)),
      aggregations.flatMap(agg => agg.attributes.map(att => att.veType.makeCVector(att.name)))
    ).flatten
  }

  def computeGroupingKeys: CodeLines = ???

  def hashStringGroupingKeys: CodeLines = ???

  def computeProjections: CodeLines = ???

  def createPartial: CFunction = CFunction(
    inputs = Nil,
    outputs = partials,
    body = {
      CodeLines.from(
        computeGroupingKeys,
        hashStringGroupingKeys,
        computeProjections,
        performGrouping
      )
    }
  )

  def performGrouping: CodeLines = ???

  def produceFinalOutputs: CodeLines = CodeLines.from(finalOutputs.map(fo => ??? : CodeLines))

  def mergePartials: CodeLines = ???

  def createFinal: CFunction = CFunction(
    inputs = partials,
    outputs = outputs,
    body = {
      CodeLines.from(hashStringGroupingKeys, performGrouping, mergePartials, produceFinalOutputs)
    }
  )

}

object StagedGroupBy {
  final case class GroupingKey(name: String, veType: VeType)
  final case class StagedProjection(name: String, veType: VeType)
  final case class StagedAggregationAttribute(name: String, veType: VeType)
  final case class StagedAggregation(
    name: String,
    finalType: VeType,
    attributes: List[StagedAggregationAttribute]
  )
}
