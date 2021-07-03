package com.nec.spark.planning
import com.nec.debugging.Debugging.RichDataSet
import com.nec.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
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

    val executionPlan = sparkSession
      .sql("SELECT AVG(value)  FROM nums")
      .as[Double]
      .debugSql(name = "AVG(value)")
      .executionPlan

    assert(
      SingleColumnAvgPlanExtractor.matchPlan(executionPlan).isDefined,
      executionPlan.toString()
    )
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

    assert(
      SingleColumnAvgPlanExtractor.matchPlan(executionPlan).isDefined,
      executionPlan.toString()
    )
  }
}
