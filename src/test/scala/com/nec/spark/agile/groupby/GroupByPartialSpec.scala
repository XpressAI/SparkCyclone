package com.nec.spark.agile.groupby

import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableInt
import com.nec.spark.agile.groupby.GroupByOutline.{GroupingKey, StagedAggregationAttribute}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.scalatest.freespec.AnyFreeSpec

class GroupByPartialSpec extends AnyFreeSpec {
  "Partial C Code works" in {
    val groupingExpressionsKeys: List[(GroupingKey, Expression)] = List(
      (GroupingKey("g1", veType = VeNullableInt), Literal.TrueLiteral)
    )

    val finalOutputs: List[Either[GroupByOutline.StagedProjection, GroupByOutline.StagedAggregation]] = List(
      Left(GroupByOutline.StagedProjection("proj", VeNullableInt)),
      Right(GroupByOutline.StagedAggregation("agg", VeNullableInt, List(StagedAggregationAttribute("foo", VeNullableInt))))
    )

    val stagedGroupBy = GroupByOutline(
      groupingKeys = groupingExpressionsKeys.map { case (gk, _) => gk },
      finalOutputs = finalOutputs
    )

    val groupByPartialGenerator = GroupByPartialGenerator(
      finalGenerator = GroupByPartialToFinalGenerator(stagedGroupBy = stagedGroupBy, computedAggregates = Nil),
      computedGroupingKeys = Nil,
      computedProjections = Nil,
      stringVectorComputations = Nil
    )

    val partialCFunction = groupByPartialGenerator
      .createPartial(inputs = List(CVector.int("foo")))
      .toCodeLinesHeaderPtr("partial_foo")

    println(partialCFunction.cCode)

    assert(partialCFunction != null)
  }
}
