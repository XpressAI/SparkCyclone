package com.nec.cmake

import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec.configuration
import com.nec.spark.SparkAdditions
import com.nec.spark.cgescape.CodegenEscapeSpec.makeCsvNumsMultiColumn
import com.nec.spark.planning.CEvaluationPlan.NativeEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import com.nec.testing.Testing.DataSize.SanityCheckSize
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

object DynamicCSqlExpressionEvaluationSpec {

  val cNativeEvaluator: NativeEvaluator = { code =>
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, code)
        .mkString("\n\n")
    )
    System.err.println(s"Generated: ${cLib.toAbsolutePath.toString}")
    new CArrowNativeInterfaceNumeric(cLib.toAbsolutePath.toString)
  }

  def configuration(sql: String): SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession =>
          new VERewriteStrategy(sparkSession, cNativeEvaluator)
        )
      )
  }

}

final class DynamicCSqlExpressionEvaluationSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions {

  "Different single-column expressions can be evaluated" - {
    List(
      "SELECT SUM(value) FROM nums" -> 62.0d,
      "SELECT SUM(value - 1) FROM nums" -> 57.0d,
      /** The below are ignored for now */
      "SELECT AVG(value) FROM nums" -> 12.4d,
      "SELECT AVG(2 * value) FROM nums" -> 24.8d,
      "SELECT AVG(2 * value), SUM(value) FROM nums" -> 0.0d,
      "SELECT AVG(2 * value), SUM(value - 1), value / 2 FROM nums GROUP BY (value / 2)" -> 0.0d
    ).take(4).foreach { case (sql, expectation) =>
      s"${sql}" in withSparkSession2(configuration(sql)) { sparkSession =>
        Source.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._
        assert(sparkSession.sql(sql).debugSqlHere.as[Double].collect().toList == List(expectation))
      }
    }
  }

  val sql_pairwise = "SELECT a + b FROM nums"
  "Support pairwise addition" in withSparkSession2(configuration(sql_pairwise)) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    assert(
      sparkSession.sql(sql_pairwise).debugSqlHere.as[(Double)].collect().toList == List(
        3,
        5,
        7,
        9,
        58
      )
    )
  }

  val sql_mci = "SELECT SUM(a + b) FROM nums"
  "Support multi-column inputs" in withSparkSession2(configuration(sql_mci)) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    assert(sparkSession.sql(sql_mci).debugSqlHere.as[(Double)].collect().toList == List(82.0))
  }

  val sql_mci_2 = "SELECT SUM(b - a) FROM nums"
  "Support multi-column inputs, order reversed" in withSparkSession2(configuration(sql_mci_2)) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      assert(sparkSession.sql(sql_mci_2).debugSqlHere.as[Double].collect().toList == List(-42.0))
  }

  val sql_mcio = "SELECT SUM(b-a), SUM(a + b) FROM nums"
  "Support multi-column inputs and outputs" in withSparkSession2(configuration(sql_mcio)) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      assert(
        sparkSession.sql(sql_mcio).debugSqlHere.as[(Double, Double)].collect().toList == List(
          -42.0 -> 82.0
        )
      )
  }

  "Different multi-column expressions can be evaluated" - {

    val sql1 = "SELECT AVG(2 * value), SUM(value) FROM nums"

    s"Multi-column: ${sql1}" in withSparkSession2(configuration(sql1)) { sparkSession =>
      Source.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._
      assert(
        sparkSession.sql(sql1).debugSqlHere.as[(Double, Double)].collect().toList == List(
          24.8 -> 62.0
        )
      )
    }

    val sql2 = "SELECT AVG(2 * value), SUM(value - 1), value / 2 FROM nums GROUP BY (value / 2)"

    s"Group by is possible with ${sql2}" ignore withSparkSession2(configuration(sql2)) {
      sparkSession =>
        Source.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._
        assert(
          sparkSession.sql(sql2).debugSqlHere.as[(Double, Double, Double)].collect().toList == Nil
        )
    }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      info(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

}
