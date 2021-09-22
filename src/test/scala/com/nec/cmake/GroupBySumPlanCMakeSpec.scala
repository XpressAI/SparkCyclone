package com.nec.cmake

import com.eed3si9n.expecty.Expecty.expect
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.VERewriteStrategy
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

final class GroupBySumPlanCMakeSpec
  extends AnyFreeSpec
  with BenchTestAdditions
  with BeforeAndAfter
  with SparkAdditions {

  private val Input: Seq[(Double, Double)] =
    Seq[(Double, Double)]((1, 5), (2, 1), (3, 9), (1, 9), (2, 78), (3, 91))

  private val ExpectedOutput = Seq((1.0, 14.0), (2.0, 79.0), (3.0, 100.0))

  private val TheSql = "SELECT nums._1, SUM(nums._2) FROM nums GROUP BY nums._1"

  "GroupBySum plan should correctly sum groups" in withSparkSession2(
    _.withExtensions(sse => sse.injectPlannerStrategy(_ => new VERewriteStrategy(CNativeEvaluator)))
  ) { sparkSession =>
    import sparkSession.implicits._

    Input
      .toDS()
      .createOrReplaceTempView("nums")

    val plan = sparkSession
      .sql(TheSql)
      .as[(Double, Double)]

    scala.Predef.assert(
      plan.queryExecution.toString().contains("sorted_idx"),
      plan.queryExecution.toString()
    )

    expect(plan.collect().toSeq.sortBy(_._1) == ExpectedOutput)
  }

  "GroupBySum plan should correctly sum groups in plain Spark" in withSparkSession2(identity) {
    sparkSession =>
      import sparkSession.implicits._

      Input
        .toDS()
        .createOrReplaceTempView("nums")

      val plan = sparkSession
        .sql(TheSql)
        .as[(Double, Double)]

      expect(plan.collect().toSeq.sortBy(_._1) == ExpectedOutput)
  }

}
