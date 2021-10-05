package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration.VeType
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
      .flatMap(_.partialTypes)

}

object StagedGroupBy {
  final case class GroupingKey(veType: VeType)
  final case class StagedProjection(veType: VeType)
  final case class StagedAggregation(finalType: VeType, partialTypes: List[VeType])
}
