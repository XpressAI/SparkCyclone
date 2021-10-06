package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.StagedGroupBy.{
  GroupingCodeGenerator,
  GroupingKey,
  StagedAggregation,
  StagedAggregationAttribute,
  StagedProjection,
  StringReference
}

//noinspection MapFlatten
final case class GroupByFunctionGeneration(
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
    val stagedGroupBy = StagedGroupBy(
      groupingKeys = veDataTransformation.groups.zipWithIndex.map {
        case (Left(stringGrouping), i) => GroupingKey(s"g_${i}", VeString)
        case (Right(typedExp), i)      => GroupingKey(s"g_${i}", typedExp.veType)
      },
      finalOutputs = veDataTransformation.outputs.zipWithIndex.map {
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByAggregation(aggregation))),
              idx
            ) =>
          Right(
            Right(
              StagedAggregation(
                outputName,
                veType,
                aggregation.partialValues(outputName).map { case (cv, ce) =>
                  StagedAggregationAttribute(name = cv.name, veType)
                }
              )
            )
          )
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByProjection(cExpression))),
              idx
            ) if veDataTransformation.groups.exists(_.right.exists(_.cExpression == cExpression)) =>
          Left(GroupingKey(outputName, veType))
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByProjection(cExpression))),
              idx
            ) =>
          Right(Left(StagedProjection(outputName, veType)))
        case (Left(NamedStringProducer(outputName, _)), idx)
            if veDataTransformation.groups.exists(_.left.exists(_.name == outputName)) =>
          Left(GroupingKey(outputName, VeString))
        case (Left(NamedStringProducer(outputName, _)), idx) =>
          Right(Left(StagedProjection(outputName, VeString)))
      }
    )

    val computeAggregate: StagedAggregation => Option[Aggregation] = agg =>
      veDataTransformation.outputs
        .lift(stagedGroupBy.finalOutputs.indexWhere(_.right.exists(_.right.exists(_ == agg))))
        .collectFirst {
          case Right(NamedGroupByExpression(name, veType, GroupByAggregation(aggregation))) =>
            aggregation
        }
    val pf = stagedGroupBy.createPartial(
      inputs = veDataTransformation.inputs,
      computeGroupingKey = gk =>
        veDataTransformation.outputs
          .lift(stagedGroupBy.groupingKeys.indexOf(gk))
          .map {
            case Left(NamedStringProducer(name, vt)) => Left(StringReference(name))
            case Right(NamedGroupByExpression(name, veType, groupByExpression)) =>
              Right(groupByExpression.fold(identity, s => sys.error(s.toString)))
          },
      computeProjection = proj =>
        veDataTransformation.outputs
          .lift(stagedGroupBy.finalOutputs.indexWhere(_.right.exists(_.left.exists(_ == proj))))
          .collectFirst {
            case Right(NamedGroupByExpression(name, veType, GroupByProjection(cExpression))) =>
              cExpression
          },
      computeAggregate = computeAggregate
    )

    val ff = stagedGroupBy.createFinal(computeAggregate = computeAggregate)

    CFunction(
      inputs = pf.inputs,
      outputs = ff.outputs,
      body = CodeLines.from(
        pf.outputs.map(cv => StagedGroupBy.declare(cv)),
        pf.body.blockCommented("Partial stage"),
        ff.body.blockCommented("Final stage"),
        pf.outputs.map(cv => StagedGroupBy.dealloc(cv))
      )
    )
  }

}
