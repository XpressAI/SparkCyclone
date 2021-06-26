package com.nec.spark.agile

import com.nec.debugging.Debugging.RichSparkPlan
import com.nec.spark.SparkAdditions
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */

final class IdentityWholeStageCodegenSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "Execute WSCE for a LocalTableScan, and get the original input back" ignore withSparkSession(
    identity
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT value FROM nums")
      .executionPlan
    val codegened = WholeStageCodegenExec(executionPlan)(1)
    info(codegened.toString())
    val result = codegened.executeCollectPublic().map(row => row.getDouble(0))
    assert(result.toList == List(1d, 2d, 3d))
  }

  "Execute Identity WSCE for a LocalTableScan, and get the original input back" in withSparkSession2(
    _.config(CODEGEN_FALLBACK.key, value = false).config("spark.sql.codegen.comments", value = true)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT value FROM nums")
      .executionPlan
    val codegened = WholeStageCodegenExec(IdentityCodegenPlan(executionPlan))(1)
    info(codegened.toString())
    codegened
      .debugCodegen(name = "identity-codegen")
    val result = codegened.executeCollectPublic().map(row => row.getDouble(0))
    assert(result.toList == List(1d, 2d, 3d))
  }

  "Execute Identity WSCE for a LocalTableScan, and get the original input back, using IdentityCodegenBatchPlan" in withSparkSession2(
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan = sparkSession
      .sql("SELECT value FROM nums")
      .executionPlan
    val codegened = WholeStageCodegenExec(IdentityCodegenBatchPlan(executionPlan))(1)
    info(codegened.toString())
    codegened
      .debugCodegen(name = "identity-codegen-batch")
    val result = codegened.executeCollectPublic().map(row => row.getDouble(0))
    assert(result.toList == List(1d, 2d, 3d))
  }
}
