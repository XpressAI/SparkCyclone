package com.nec.spark.agile

import com.nec.debugging.Debugging.RichDataSet

import java.util.Properties
import com.nec.h2.H2DatabaseConnector
import com.nec.spark.SampleTestData
import com.nec.spark.SparkAdditions
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.aggregate.VEHashAggregate
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class RealWorldSparkPlansSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {
  before {
    H2DatabaseConnector.init()
  }
  "We get the spark plan for more complex query" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val props = new Properties()

    sparkSession.read
      .jdbc(H2DatabaseConnector.inputH2Url, "Users", props)
      .createOrReplaceTempView("Users")

    sparkSession.read
      .option("header", "true")
      .csv(SampleTestData.OrdersCsv.toString)
      .createOrReplaceTempView("Orders")

    sparkSession
      .sql(sqlText =
        "SELECT SUM(totalPrice), userId, joined_timestamp FROM Users JOIN Orders on Orders.userId = Users.id " +
          "GROUP BY userId, joined_timestamp ORDER BY joined_timestamp ASC"
      )
      .debugSqlAndShow(name = "real-life-example")
  }
  "We get the spark plan for a simple sum" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._

    sparkSession.read
      .option("header", "true")
      .csv(SampleTestData.OrdersCsv.toString)
      .createOrReplaceTempView("Orders")

    val result = sparkSession
      .sql(sqlText = "SELECT SUM(totalPrice) FROM Orders")
      .debugSqlAndShow(name = "sum-total-order-price")
      .as[Double]
      .collect()
      .head

    assert(result == 16305.0)
  }
  "We get the spark plan for a simple sum rewritten" in withSparkSession2(
    _.withExtensions(extensions =>
      extensions.injectColumnar({ sparkSession =>
        new ColumnarRule {
          override def preColumnarTransitions: Rule[SparkPlan] = sparkPlan =>
            PartialFunction
              .condOpt(sparkPlan) {
                case first @ HashAggregateExec(
                      requiredChildDistributionExpressions,
                      groupingExpressions,
                      Seq(AggregateExpression(avg @ Sum(exr), mode, isDistinct, filter, resultId)),
                      aggregateAttributes,
                      initialInputBufferOffset,
                      resultExpressions,
                      cc @ org.apache.spark.sql.execution.exchange
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
                    ) if (avg.references.size == 1) =>
                  println("MATCHED!!!!")
                  first.copy(child =
                    cc.copy(child =
                      VEHashAggregate(
                        _requiredChildDistributionExpressions,
                        _groupingExpressions,
                        _aggregateExpressions,
                        _aggregateAttributes,
                        _initialInputBufferOffset,
                        _resultExpressions,
                        fourth
                      )
                    )
                  )
              }
              .getOrElse(sparkPlan)
        }
      })
    )
  ) { sparkSession =>
    import sparkSession.implicits._

    sparkSession.read
      .option("header", "true")
      .csv(SampleTestData.OrdersCsv.toString)
      .createOrReplaceTempView("Orders")

    val result = sparkSession
      .sql(sqlText = "SELECT SUM(totalPrice) FROM Orders")
      .debugSqlAndShow(name = "sum-total-order-price-2")
      .as[Double]
      .collect()
      .head

    assert(result == 16305.0)
  }
}
