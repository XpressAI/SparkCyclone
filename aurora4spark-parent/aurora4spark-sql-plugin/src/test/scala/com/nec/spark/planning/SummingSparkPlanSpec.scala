package com.nec.spark.planning

import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

final class SummingSparkPlanSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "Sum plan matches sum of two columns" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double)]((1, 2), (3, 4), (5, 6))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession.sql("SELECT SUM(_1 + _2) FROM nums").as[Double].executionPlan
    assert(SumPlanExtractor.matchPlan(executionPlan).isDefined, executionPlan.toString())
  }

  "Sum plan matches sum of three columns" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan =
      sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums").as[Double].executionPlan
    assert(SumPlanExtractor.matchPlan(executionPlan).isDefined, executionPlan.toString())
  }

  "Sum plan matches two sum queries" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(_1), SUM(_1 +_2) FROM nums")
      .as[(Double, Double)]
      .executionPlan
    assert(SumPlanExtractor.matchPlan(executionPlan).isDefined, executionPlan.toString())
  }

  "Sum plan matches three sum queries" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(_1), SUM(_1 +_2), SUM(_3) FROM nums")
      .as[(Double, Double, Double)]
      .executionPlan
    assert(SumPlanExtractor.matchPlan(executionPlan).isDefined, executionPlan.toString())
  }

  "Sum plan extracts correct numbers flattened for three columns" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (4, 5, 6), (7, 8, 9))
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan =
      sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums").as[Double].executionPlan
    assert(
      SumPlanExtractor
        .matchPlan(executionPlan)
        .contains(List(1d, 4d, 7d, 2d, 5d, 8d, 3d, 6d, 9d))
    )
  }

  "Summing plan does not match in the averaging plan" in withSparkSession(identity) {
    sparkSession =>
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      val executionPlan = sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].executionPlan

      assert(AveragingPlanner.matchPlan(executionPlan).isEmpty, executionPlan.toString())
  }

  "Summing plan does not match the averaging plan" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].executionPlan

    assert(SumPlanExtractor.matchPlan(executionPlan).isEmpty, executionPlan.toString())
  }

  "We can sum multiple columns reading from Parquet" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
      .set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoSumPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          MultipleColumnsSummingPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer.UnsafeBased,
            childPlan.attributes
          )
        }
        .getOrElse(fail(s"Not expected to be here: ${sparkPlan}"))
    }

    val sumDataSet = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a)")
      .as[Double]
      .debugConditionally()

    assert(sumDataSet.collect().toList == List(62))
  }

}
