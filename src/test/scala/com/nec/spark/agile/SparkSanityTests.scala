package com.nec.spark.agile

import com.nec.spark.Aurora4SparkDriverPlugin
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.SingleColumnSumPlanExtractor
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class SparkSanityTests
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We can run a Spark-SQL job for a sanity test" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
    assert(result == List(3))
  }

  "From the execution plan, we get the inputted numbers" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(value)  FROM nums")
      .as[Double]
      .executionPlan

    assert(
      executionPlan.getClass.getCanonicalName
        == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
    )

    assert(
      SingleColumnSumPlanExtractor
        .matchPlan(executionPlan)
        .map(_.sparkPlan.execute())
        .map(_.collect())
        .map(_.map(_.getDouble(0)).toList) == Some(List(1, 2, 3))
    )
  }
}
