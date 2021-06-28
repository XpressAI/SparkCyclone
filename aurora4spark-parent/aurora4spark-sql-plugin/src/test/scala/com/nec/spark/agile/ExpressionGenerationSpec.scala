package com.nec.spark.agile
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.Metadata
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import CExpressionEvaluation._
object ExpressionGenerationSpec {

}

final class ExpressionGenerationSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  import com.eed3si9n.expecty.Expecty.assert
  "SUM((value#14 - 1.0)) is evaluated" in {
    val expr = AggregateExpression(
      aggregateFunction = Sum(
        Subtract(
          AttributeReference(
            name = "abcd",
            dataType = DoubleType,
            nullable = false,
            metadata = Metadata.empty
          )(),
          Literal(1.0, DoubleType)
        )
      ),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(
      cGen(Alias(null, "summy")(), expr) ==
        List(
          "double summy_accumulated = 0;",
          "for (int i = 0; i < input->count; i++) {",
          "summy_accumulated += input->data[i] - 1.0;",
          "}",
          "double summy_result = summy_accumulated;"
        )
    )

  }

  "AVG((value#14 - 1.0)) is evaluated" in {
    val expr = AggregateExpression(
      aggregateFunction = Average(
        Subtract(
          AttributeReference(
            name = "abcd",
            dataType = DoubleType,
            nullable = false,
            metadata = Metadata.empty
          )(),
          Literal(1.0, DoubleType)
        )
      ),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(
      cGen(Alias(null, "avy#123 + 51")(), expr) == List(
        "double avy12351_accumulated = 0;",
        "int avy12351_counted = 0;",
        "for (int i = 0; i < input->count; i++) {",
        "avy12351_accumulated += input->data[i] - 1.0;",
        "avy12351_counted += 1;",
        "}",
        "double avy12351_result = avy12351_accumulated / avy12351_counted;"
      )
    )

  }
  "AVG((value#14 + 2.0)) is evaluated" in {
    val expr = AggregateExpression(
      aggregateFunction = Average(
        Add(
          AttributeReference(
            name = "abcd",
            dataType = DoubleType,
            nullable = false,
            metadata = Metadata.empty
          )(),
          Literal(2.0, DoubleType)
        )
      ),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(
      cGen(Alias(null, "avy#123 + 2")(), expr) ==
        List(
          "double avy1232_accumulated = 0;",
          "int avy1232_counted = 0;",
          "for (int i = 0; i < input->count; i++) {",
          "avy1232_accumulated += input->data[i] + 2.0;",
          "avy1232_counted += 1;",
          "}",
          "double avy1232_result = avy1232_accumulated / avy1232_counted;"
        )
    )

  }

  "Different expressions are found" - {
    List(
      "SELECT SUM(value) FROM nums",
      "SELECT SUM(value - 1) FROM nums",
      "SELECT AVG(2 * value) FROM nums",
      "SELECT AVG(2 * value), SUM(value) FROM nums",
      "SELECT AVG(2 * value), SUM(value - 1), value / 2 FROM nums GROUP BY (value / 2)"
    ).take(2).foreach { sql =>
      s"${sql}" in withSparkSession2(
        _.config(CODEGEN_FALLBACK.key, value = false)
          .config("spark.sql.codegen.comments", value = true)
          .withExtensions(sse =>
            sse.injectPlannerStrategy(sparkSession =>
              new Strategy {
                override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                  plan match {
                    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                      info(s"Grouping ==> ${groupingExpressions.mkString}")
                      info(s"Result ==> ${resultExpressions.mkString}")
                      info(s"Result ==> ${resultExpressions.collect {
                        case Alias(ae @ AggregateExpression(Sum(sth), mode, isDistinct, filter, resultId), name) =>
                          info(sth.toString)
                          info(sth.getClass.getCanonicalName)
                          ae.productIterator.mkString("|")
                      }.mkString}")
                      Nil
                    case _ => Nil
                  }
              }
            )
          )
      ) { sparkSession =>
        Source.CSV.generate(sparkSession)
        import sparkSession.implicits._
        sparkSession.sql(sql).debugSqlHere
      }
    }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      info(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

}
