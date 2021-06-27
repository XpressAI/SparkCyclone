package com.nec.spark.agile

import com.nec.debugging.Debugging.RichDataSet
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowSummingCodegenPlan
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

final class ArrowSummingCodegenPlanSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {
  "Execute Identity WSCG for a SUM() VeSummingCodegenPlan" in withSparkSession2(
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession =>
          new Strategy {
            override def apply(plan: LogicalPlan): Seq[SparkPlan] =
              plan match {
                case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                  List(ArrowSummingCodegenPlan(planLater(child), ArrowSummer.JVMBased))
                case _ => Nil
              }
          }
        )
      )
  ) { sparkSession =>
    import sparkSession.implicits._
    sparkSession.read.parquet("/Users/wosin/aurora4spark/aurora4spark-parent/aurora4spark-sql-plugin/src/test/resources/com/nec/spark/parquet-arrow-example.parquet")
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT SUM(a) FROM nums")
      .debugSqlAndShow(name = "arrow-sum-codegen")
      .as[Double]

    println(executionPlan.queryExecution.sparkPlan)
    val result = executionPlan.collect().toList
    assert(result == List(6d))
  }
}
