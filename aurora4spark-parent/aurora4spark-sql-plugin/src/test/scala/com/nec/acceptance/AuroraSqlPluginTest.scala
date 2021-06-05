package com.nec.acceptance

import com.nec.spark.SparkAdditions
import com.nec.spark.planning.AveragingPlanOffHeap.OffHeapDoubleAverager.UnsafeBased
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.{SingleColumnAvgPlanExtractor, SparkSqlPlanExtension, SummingPlanOffHeap}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfter

final class AuroraSqlPluginTest
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We call VE with our Averaging plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
  ) { sparkSession =>
    markup("AVG([Double])")
    import sparkSession.implicits._

    val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))
    info(s"Input: ${nums}")

    nums
      .toDS()
      .createOrReplaceTempView("nums")
    SparkSqlPlanExtension.rulesToApply.clear()
    SparkSqlPlanExtension.rulesToApply.append { (sparkPlan) =>
      SingleColumnAvgPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          SummingPlanOffHeap(
            childPlan.sparkPlan,
            MultipleColumnsOffHeapSummer.UnsafeBased,
            childPlan.column
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT AVG(value) FROM nums").as[Double]

    val result = sumDataSet.head()

    assert(result == nums.sum / nums.length)
  }

}
