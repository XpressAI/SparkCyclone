package com.nec.spark.agile

import com.nec.debugging.Debugging.RichDataSet
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import com.nec.spark.agile.wscg.ArrowSummingCodegenPlan
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

final class ArrowSummingCodegenPlanSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {
  "Execute Identity WSCG for a SUM() VeSummingCodegenPlan" in withSparkSession2(
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectColumnar(ss =>
          new ColumnarRule {
            override def postColumnarTransitions: Rule[SparkPlan] = { sparkPlan =>
              PartialFunction
                .condOpt(sparkPlan) {
                  case first @ HashAggregateExec(
                        requiredChildDistributionExpressions,
                        groupingExpressions,
                        Seq(
                          AggregateExpression(avg @ Sum(exr), mode, isDistinct, filter, resultId)
                        ),
                        aggregateAttributes,
                        initialInputBufferOffset,
                        resultExpressions,
                        see @ org.apache.spark.sql.execution.exchange
                          .ShuffleExchangeExec(
                            outputPartitioning,
                            org.apache.spark.sql.execution.aggregate
                              .HashAggregateExec(
                                _requiredChildDistributionExpressions,
                                _groupingExpressions,
                                _aggregateExpressions,
                                _aggregateAttributes,
                                _initialInputBufferOffset,
                                _resultExpressions,
                                fourth
                              ),
                            shuffleOrigin
                          )
                      ) if (avg.references.size == 1) => {
                    println(fourth)
                    println(fourth.getClass.getCanonicalName())
                    println(fourth.supportsColumnar)
                    val indices = fourth.output.map(_.name).zipWithIndex.toMap
                    val colName = avg.references.head.name

                    first.copy(child =
                      see.copy(child = ArrowSummingCodegenPlan(fourth, ArrowSummer.JVMBased))
                    )
                  }
                }
                .getOrElse(sparkPlan)
            }
          }
        )
      )
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(value) FROM nums")
      .debugSqlAndShow(name = "arrow-sum-codegen")
      .as[Double]
    val result = executionPlan.collect().toList
    assert(result == List(6d))
  }
  "Execute Identity WSCG for a SUM() VeSummingCodegenPlan with Columnar/Parquet input" in withSparkSession2(
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectColumnar(ss =>
          new ColumnarRule {
            override def postColumnarTransitions: Rule[SparkPlan] = { sparkPlan =>
              PartialFunction
                .condOpt(sparkPlan) {
                  case first @ HashAggregateExec(
                        requiredChildDistributionExpressions,
                        groupingExpressions,
                        Seq(
                          AggregateExpression(avg @ Sum(exr), mode, isDistinct, filter, resultId)
                        ),
                        aggregateAttributes,
                        initialInputBufferOffset,
                        resultExpressions,
                        see @ org.apache.spark.sql.execution.exchange
                          .ShuffleExchangeExec(
                            outputPartitioning,
                            org.apache.spark.sql.execution.aggregate
                              .HashAggregateExec(
                                _requiredChildDistributionExpressions,
                                _groupingExpressions,
                                _aggregateExpressions,
                                _aggregateAttributes,
                                _initialInputBufferOffset,
                                _resultExpressions,
                                fourth
                              ),
                            shuffleOrigin
                          )
                      ) if (avg.references.size == 1) => {
                    println(fourth)
                    println(fourth.getClass.getCanonicalName())
                    println(fourth.supportsColumnar)
                    val indices = fourth.output.map(_.name).zipWithIndex.toMap
                    val colName = avg.references.head.name

                    first.copy(child =
                      see.copy(child = ArrowSummingCodegenPlan(fourth, ArrowSummer.JVMBased))
                    )
                  }
                }
                .getOrElse(sparkPlan)
            }
          }
        )
      )
  ) { sparkSession =>
    import sparkSession.implicits._
    sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .createOrReplaceTempView("nums")
    val executionPlan = sparkSession
      .sql("SELECT SUM(a) FROM nums")
      .debugSqlAndShow(name = "arrow-sum-codegen-parquet")
      .as[Double]
    val result = executionPlan.collect().toList
    assert(result == List(6d))
  }

}
