package com.nec.acceptance

import com.nec.spark.SparkAdditions
import com.nec.spark.planning.AveragingPlanOffHeap.OffHeapDoubleAverager.UnsafeBased
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.{AveragingPlanOffHeap, SingleColumnAvgPlanExtractor, SparkSqlPlanExtension, SummingPlanOffHeap}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED

final class AuroraSqlPluginTest
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We call VE with our Averaging plan" in withSparkSession({
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  }
  ) { sparkSession =>
    markup("AVG([Double])")
    import sparkSession.implicits._

    val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))
    info(s"Input: ${nums}")

    nums
      .toDS()
      .createOrReplaceTempView("nums")
    SparkSqlPlanExtension.rulesToApply.clear()
    SparkSqlPlanExtension.rulesToApply.append(
      new Rule[SparkPlan] {
        override def apply(plan: SparkPlan): SparkPlan = {
          SingleColumnAvgPlanExtractor
            .matchPlan(plan)
            .map { childPlan =>
              AveragingPlanOffHeap(
                childPlan.sparkPlan,
                MultipleColumnsOffHeapSummer.UnsafeBased,
                childPlan.column
              )
            }
            .getOrElse(fail("Not expected to be here"))
        }
      })


    val sumDataSet =
      sparkSession.sql("SELECT AVG(value) FROM nums").as[Double]

    val result = sumDataSet.head()

    assert(result == nums.sum / nums.length)
  }

}
