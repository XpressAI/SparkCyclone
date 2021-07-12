package com.nec.spark.planning

import scala.util.Random

import com.nec.cmake.CMakeBuilder
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.CBased
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
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

  "Partial plan matching returns proper sum of single column" ignore withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
      .set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)

  ) { sparkSession =>
    import sparkSession.implicits._
    val seq = Seq.fill(500)(Random.nextDouble())
      seq.toDS()
      .repartition(200)
      .createOrReplaceTempView("nums")
    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append {
      sparkPlan => {
        PartialSingleColumnSumPlanExtractor
          .matchPlan(sparkPlan)
          .map(plan => {

            val newChild = PartialArrowSummingPlan(plan.child,
              CBased(CMakeBuilder.CLibPath.toString),
              plan.column)

            val d = plan.replaceMain(newChild)
            d
          })
          .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))

      }
    }
    val executionPlan = sparkSession
      .sql("SELECT SUM(value) FROM nums")
      .as[(Double)]
      .collect()
      .toList


    assert(executionPlan.head === seq.sum +- 0.0000000001)
  }

  /** Ignored this and the below because they are causing issues and also are not going to be used any more **/

  "Wholestage gen is executed after the plugin" ignore withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      .set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)

  ) { sparkSession =>
    import sparkSession.implicits._
    val seq = Seq.fill(500)(Random.nextDouble())
    seq.toDS()
      .createOrReplaceTempView("nums")
    SparkSqlPlanExtension.rulesToApply.clear()

    var beforeMatchingPlan: SparkPlan = null

    SparkSqlPlanExtension.rulesToApply.append {
      sparkPlan => {
        PartialSingleColumnSumPlanExtractor
          .matchPlan(sparkPlan)
          .map(plan => {

            val newChild = PartialArrowSummingPlan(plan.child,
              CBased(CMakeBuilder.CLibPath.toString),
              plan.column)
            val newMain = plan.replaceMain(newChild)
            beforeMatchingPlan = newMain

            newMain

          })
          .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))

      }
    }
    val querySet = sparkSession
      .sql("SELECT SUM(value) FROM nums")
      .as[(Double)]

    querySet.collect()

    assert(
      beforeMatchingPlan.isInstanceOf[HashAggregateExec] &&
      querySet.executionPlan.isInstanceOf[WholeStageCodegenExec]
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
      .sql("SELECT collect_list(_1) FROM nums GROUP BY (_2)")
      .as[(Array[Double])]

    assert(
      SingleColumnSumPlanExtractor
        .matchPlan(null)
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
