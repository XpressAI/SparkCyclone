package com.nec.spark.agile

import com.nec.cmake.CMakeBuilder
import com.nec.spark.planning.ArrowSummingPlanOffHeap
import com.nec.spark.planning.ArrowVeoSumPlanExtractor
import com.nec.spark.planning.SparkSqlPlanExtension
import com.nec.spark.planning.SumPlanExtractor
import com.nec.spark.Aurora4SparkDriver
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowSummingPlanOffHeap.OffHeapSummer.CBased
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class SparkSanityTests
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "It is not launched by default" in withSpark(identity) { _ =>
    assert(!Aurora4SparkDriver.launched, "Expect the driver to have not been launched")
    assert(
      !Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.isEmpty,
      "Expect the executor plugin to have not been launched"
    )
  }

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

    val executionPlan =
      sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].executionPlan

    assert(
      executionPlan.getClass.getCanonicalName
        == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
    )

    assert(
      SumPlanExtractor
        .matchPlan(executionPlan)
        .contains(List(1, 2, 3))
    )
  }

  "We handle single column sum with specific plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      ArrowVeoSumPlanExtractor
        .matchPlan(sparkPlan)
        .map(plan =>
          ArrowSummingPlanOffHeap(
            plan.sparkPlan,
            CBased(CMakeBuilder.CLibPath.toString),
            plan.column
          )
        )
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val inputData: Seq[(Double, Double)] = Seq((10, 2), (20, 5), (30, 6), (40, 7))

    val sumDataSet2 = inputData
      .toDS()
      .as[(Double, Double)]
      .selectExpr("SUM(_1)")
      .as[Double]
      .debugConditionally()

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(100.0))
  }

}
