package com.nec.spark.planning

import com.nec.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

final class SummingSparkPlanSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "Specific plan matches sum of a single column" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(_1) FROM nums")
      .as[(Double)]
      .executionPlan
    assert(
      SingleColumnSumPlanExtractor
        .matchPlan(executionPlan)
        .isDefined,
      executionPlan.toString()
    )
  }

  "Specific plan doesn't match sum of two columns" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(_1 + _2) FROM nums")
      .as[(Double)]
      .executionPlan
    assert(
      SingleColumnSumPlanExtractor
        .matchPlan(executionPlan)
        .isEmpty,
      executionPlan.toString()
    )
  }

  "Summing plan does not match in the averaging plan" in withSparkSession(identity) {
    sparkSession =>
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      val executionPlan = sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].executionPlan

      assert(SingleColumnAvgPlanExtractor.matchPlan(executionPlan).isEmpty, executionPlan.toString())
  }

  "Summing plan does not match the averaging plan" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].executionPlan

    assert(SingleColumnSumPlanExtractor.matchPlan(executionPlan).isEmpty, executionPlan.toString())
  }
}
