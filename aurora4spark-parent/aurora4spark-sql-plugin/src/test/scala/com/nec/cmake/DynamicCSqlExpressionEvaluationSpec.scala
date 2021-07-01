package com.nec.cmake

import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec.configuration
import com.nec.spark.SparkAdditions
import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.planning.CEvaluationPlan
import com.nec.spark.planning.CEvaluationPlan.NativeEvaluator
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

object DynamicCSqlExpressionEvaluationSpec {

  val cNativeEvaluator: NativeEvaluator = { code =>
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, code)
        .mkString("\n\n")
    )
    System.err.println(s"Generated: ${cLib.toAbsolutePath.toString}")
    new CArrowNativeInterfaceNumeric(cLib.toAbsolutePath.toString)
  }

  def configuration(sql: String): SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession =>
          new Strategy {
            override def apply(plan: LogicalPlan): Seq[SparkPlan] =
              plan match {
                case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                  List(
                    CEvaluationPlan(
                      List(
                        List(
                          "extern \"C\" long f(non_null_double_vector* input, non_null_double_vector* output) {",
                          "output->data = (double *)malloc(1 * sizeof(double));"
                        ),
                        CExpressionEvaluation.cGen(
                          resultExpressions.head.asInstanceOf[Alias],
                          resultExpressions.head
                            .asInstanceOf[Alias]
                            .child
                            .asInstanceOf[AggregateExpression]
                        ),
                        List("}")
                      ).flatten,
                      planLater(child),
                      cNativeEvaluator
                    )
                  )
                case _ => Nil
              }
          }
        )
      )
  }

}

final class DynamicCSqlExpressionEvaluationSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions {

  "Different expressions can be evaluated" - {
    List(
      "SELECT SUM(value) FROM nums" -> 62.0d,
      "SELECT SUM(value - 1) FROM nums" -> 57.0d,
      /** The below are ignored for now */
      "SELECT AVG(value) FROM nums" -> 24.8d,
      "SELECT AVG(2 * value) FROM nums" -> 24.8d,
      "SELECT AVG(2 * value), SUM(value) FROM nums" -> 0.0d,
      "SELECT AVG(2 * value), SUM(value - 1), value / 2 FROM nums GROUP BY (value / 2)" -> 0.0d
    ).take(2).foreach { case (sql, expectation) =>
      s"${sql}" in withSparkSession2(configuration(sql)) { sparkSession =>
        Source.CSV.generate(sparkSession)
        import sparkSession.implicits._
        assert(sparkSession.sql(sql).debugSqlHere.as[Double].collect().toList == List(expectation))
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
