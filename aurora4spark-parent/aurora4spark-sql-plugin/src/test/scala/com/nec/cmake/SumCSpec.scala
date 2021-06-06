package com.nec.cmake

import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowSummingPlan
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.CBased
import com.nec.spark.planning.SingleColumnSumPlanExtractor
import com.nec.spark.planning.SparkSqlPlanExtension
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

final class SumCSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions with Matchers {
  "We handle single column sum with specific plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      SingleColumnSumPlanExtractor
        .matchPlan(sparkPlan)
        .map(plan =>
          ArrowSummingPlan(
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
