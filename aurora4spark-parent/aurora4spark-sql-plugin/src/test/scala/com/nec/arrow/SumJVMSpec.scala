package com.nec.arrow

import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowSummingPlan
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.CBased
import com.nec.spark.planning.SingleColumnSumPlanExtractor
import com.nec.spark.planning.SparkSqlPlanExtension
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

final class SumJVMSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions with Matchers {
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
          ArrowSummingPlan(plan.sparkPlan, ArrowSummer.JVMBased, plan.column)
        )
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val sumDataSet2 = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a)")
      .as[Double]
      .debugConditionally()

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(62.0))
  }
}
