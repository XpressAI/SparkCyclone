package com.nec.spark.agile.groupby

import com.nec.spark.agile.CFunctionGeneration.{CExpression, TypedCExpression2}
import com.nec.spark.agile.core._
import com.nec.spark.agile.StringHole.StringHoleEvaluation.LikeStringHoleEvaluation
import com.nec.spark.agile.groupby.GroupByOutline.{GroupingKey, StagedAggregationAttribute, StagedProjection, StringReference}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.scalatest.freespec.AnyFreeSpec

class GroupByPartialSpec extends AnyFreeSpec {
  "Partial C Code works" in {
    val inputs = List(CVector.int("value_0"), CVector.int("value_1"))

    val proj = "proj"
    val groupingExpressionsKeys: List[(GroupingKey, Expression)] = List(
      (GroupingKey("grouping_key", veType = VeNullableInt), Literal.TrueLiteral)
    )
    val agg = "agg"
    val computedGroupingKeys = List(
      GroupingKey(agg, VeNullableInt) -> Right(TypedCExpression2(VeNullableInt, CExpression("foo", None))),
      GroupingKey(agg+"2", VeNullableInt) -> Right(TypedCExpression2(VeNullableInt, CExpression("foo2", None))),
      //TODO: String references are just not supported at the moment
      //GroupingKey("str_key", VeNullableInt) -> Left(StringReference("some_str_col"))
    )

    val computedProjections = List(
      StagedProjection(proj, VeNullableDouble) -> Right(TypedCExpression2(VeNullableDouble, CExpression("bar", None))),
    )

    val stringVectorComputations = List(
      //TODO: StringHole seems to not be fully supported in aggregation yet
      //LikeStringHoleEvaluation("spam", "eggs")
    )

    val finalOutputs: List[Either[GroupByOutline.StagedProjection, GroupByOutline.StagedAggregation]] = List(
      Left(GroupByOutline.StagedProjection(proj, VeNullableDouble)),
      Right(GroupByOutline.StagedAggregation("some_name", VeNullableInt, List(StagedAggregationAttribute(agg, VeNullableInt))))
    )

    val stagedGroupBy = GroupByOutline(
      groupingKeys = groupingExpressionsKeys.map { case (gk, _) => gk },
      finalOutputs = finalOutputs
    )

    val groupByPartialGenerator = GroupByPartialGenerator(
      finalGenerator = GroupByPartialToFinalGenerator(stagedGroupBy = stagedGroupBy, computedAggregates = Nil),
      computedGroupingKeys = computedGroupingKeys,
      computedProjections = computedProjections,
      stringVectorComputations = stringVectorComputations
    )

    val partialCFunction = groupByPartialGenerator
      .createPartial(inputs = inputs)
      .toCodeLinesHeaderBatchPtr("partial_foo")

    println(partialCFunction.cCode)

    assert(partialCFunction != null)
  }
}
