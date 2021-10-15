package com.nec.cmake.eval

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.GroupingCodeGenerator
import com.nec.spark.agile.StringProducer.CopyStringProducer
import com.nec.spark.agile.groupby.GroupByOutline.{
  GroupingKey,
  StagedAggregation,
  StagedAggregationAttribute,
  StagedProjection,
  StringReference
}
import com.nec.spark.agile.groupby.{
  GroupByOutline,
  GroupByPartialGenerator,
  GroupByPartialToFinalGenerator
}
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
      } yield GroupByPartialGenerator(GroupByPartialToFinalGenerator(stagedGroupBy, ca), gks, cpr)
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
        pf.body.blockCommented("Perform the Partial computation stage"),
        ff.body.blockCommented("Perform the Final computation stage"),
        pf.outputs
          .map(cv => GroupByOutline.dealloc(cv))
          .blockCommented("Deallocate the partial variables")
      )
    )
  }

}
