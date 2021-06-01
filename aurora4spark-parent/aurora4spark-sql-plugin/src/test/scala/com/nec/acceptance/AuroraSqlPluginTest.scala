package com.nec.acceptance

import com.nec.spark.SparkAdditions
import com.nec.spark.planning.AveragingPlanner
import com.nec.spark.planning.AveragingSparkPlanMultipleColumns
import com.nec.spark.planning.SparkSqlPlanExtension
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
      AveragingPlanner
        .matchPlan(sparkPlan)
        .map { childPlan =>
          AveragingSparkPlanMultipleColumns(
            childPlan.sparkPlan,
            childPlan.attributes,
            AveragingSparkPlanMultipleColumns.averageLocalScala
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT AVG(value) FROM nums").as[Double]

    val result = sumDataSet.head()

    assert(result == nums.sum / nums.length)
  }

  "We call VE with our Averaging plan for multiple average operations" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
  ) { sparkSession =>
    markup("AVG([Double]), AVG([Double]), AVG([Double])")
    import sparkSession.implicits._

    val nums: List[(Double, Double, Double)] = List((1, 2, 3), (4, 5, 6), (7, 8, 9))
    info(s"Input: $nums")

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { (sparkPlan) =>
      AveragingPlanner
        .matchPlan(sparkPlan)
        .map { childPlan =>
          AveragingSparkPlanMultipleColumns(
            childPlan.sparkPlan,
            childPlan.attributes,
            AveragingSparkPlanMultipleColumns.averageLocalScala
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession
        .sql("SELECT AVG(_1), AVG(_2), AVG(_3) FROM nums")
        .as[(Double, Double, Double)]

    val expected = (
      nums.map(_._1).sum / nums.size,
      nums.map(_._2).sum / nums.size,
      nums.map(_._3).sum / nums.size
    )

    val result = sumDataSet.collect().head

    assert(result == expected)
  }

}
