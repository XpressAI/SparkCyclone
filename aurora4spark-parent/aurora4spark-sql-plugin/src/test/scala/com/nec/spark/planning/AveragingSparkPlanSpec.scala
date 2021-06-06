package com.nec.spark.planning
import com.nec.cmake.CMakeBuilder
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.CBased
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

final class AveragingSparkPlanSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "Specific plan matches single column average" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan =
      sparkSession.sql("SELECT AVG(value)  FROM nums").as[(Double)].executionPlan

    assert(SingleColumnAvgPlanExtractor.matchPlan(executionPlan).isDefined, executionPlan.toString())
  }

  "Specific plugin does not match average of sum" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan =
      sparkSession.sql("SELECT AVG(value + value) FROM nums").as[(Double)].executionPlan

    assert(SingleColumnAvgPlanExtractor.matchPlan(executionPlan).isDefined, executionPlan.toString())
  }

  "We extract data with RowToColumnarExec" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))

    SparkSqlPlanExtension.rulesToApply.clear()

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      SingleColumnAvgPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          ArrowAveragingPlan(
            RowToColumnarExec(childPlan.sparkPlan),
            CBased(CMakeBuilder.CLibPath.toString),
            childPlan.column
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession
        .sql("SELECT AVG(value) FROM nums")
        .as[Double]
        .debugConditionally()

    val listOfDoubles = sumDataSet.collect().toList
    info(listOfDoubles.toString())

    val result = listOfDoubles.head
    assert(result == nums.sum / nums.length)
  }
}
