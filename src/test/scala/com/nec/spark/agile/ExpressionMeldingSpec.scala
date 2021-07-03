package com.nec.spark.agile
import com.nec.spark.planning.VERewriteStrategy.meldAggregateAndProject
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec

final class ExpressionMeldingSpec extends AnyFreeSpec {
  "(sum(value#4) AS sum(value)#13) x (a#0 AS value#4) = (sum(a#0) AS sum(value)#13)" in {

    val value4 = AttributeReference("value", DoubleType)(ExprId.apply(4))
    val a0 = AttributeReference("a", DoubleType)(ExprId.apply(0))

    val expected = Alias(
      child = AggregateExpression(Sum(a0), Complete, isDistinct = false, filter = None),
      name = "sum(value)"
    )(ExprId(13))

    val inputColumnsA: List[NamedExpression] = List(
      Alias(
        child = AggregateExpression(Sum(value4), Complete, isDistinct = false, filter = None),
        name = "sum(value)"
      )(ExprId(13))
    )

    val inputColumnsB: List[NamedExpression] =
      List(Alias(child = a0, name = "value")(ExprId(4)))

    val result1 = meldAggregateAndProject(inputColumnsA, inputColumnsB).head

    assert(result1.semanticEquals(expected))
  }
}
