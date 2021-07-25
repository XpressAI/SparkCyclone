package com.nec.cmake

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec.configuration
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.NativeCsvExec.NativeCsvStrategy
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.SampleSource
import com.nec.testing.SampleSource.SampleColA
import com.nec.testing.SampleSource.SampleColB
import com.nec.testing.SampleSource.makeCsvNumsMultiColumn
import com.nec.testing.Testing.DataSize.SanityCheckSize
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

object DynamicCSqlExpressionEvaluationSpec {

  def configuration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession =>
          new VERewriteStrategy(sparkSession, CNativeEvaluator)
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
      s"SELECT SUM(${SampleColA}) FROM nums" -> 62.0d,
      s"SELECT SUM(${SampleColA} - 1) FROM nums" -> 57.0d,
      /** The below are ignored for now */
      s"SELECT AVG(${SampleColA}) FROM nums" -> 12.4d,
      s"SELECT AVG(2 * ${SampleColA}) FROM nums" -> 24.8d,
      s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA}) FROM nums" -> 0.0d,
      s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA} - 1), ${SampleColA} / 2 FROM nums GROUP BY (${SampleColA} / 2)" -> 0.0d
    ).zipWithIndex.take(4).foreach { case ((sql, expectation), idx) =>
      s"(n${idx}) ${sql}" in withSparkSession2(configuration) { sparkSession =>
        SampleSource.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._
        sparkSession.sql(sql).ensureCEvaluating().debugSqlHere { ds =>
          assert(ds.as[Double].collect().toList == List(expectation))
        }
      }
    }
  }

  val sql_pairwise = s"SELECT ${SampleColA} + ${SampleColB} FROM nums"
  "Support pairwise addition" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    sparkSession.sql(sql_pairwise).ensureCEvaluating().debugSqlHere { ds =>
      assert(ds.as[(Double)].collect().toList.sorted == List[Double](3, 5, 7, 9, 58).sorted)
    }
  }

  val sql_mci = s"SELECT SUM(${SampleColA} + ${SampleColB}) FROM nums"
  "Support multi-column inputs" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    sparkSession.sql(sql_mci).ensureCEvaluating().debugSqlHere { ds =>
      assert(ds.as[(Double)].collect().toList == List(82.0))
    }
  }

  val sql_mci_2 = s"SELECT SUM(${SampleColB} - ${SampleColA}) FROM nums"
  "Support multi-column inputs, order reversed" in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      sparkSession.sql(sql_mci_2).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[Double].collect().toList == List(-42.0))
      }
  }

  val sql_cnt = s"SELECT COUNT(*) FROM nums"
  "Support count"  in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      sparkSession.sql(sql_cnt).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[Long].collect().toList == List(5))
      }
  }

  val sql_cnt_multiple_ops = s"SELECT COUNT(*), SUM(${SampleColB} - ${SampleColA}) FROM nums"
  "Support count with other operations in the same query"  in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      sparkSession.sql(sql_cnt_multiple_ops).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Long, Double)].collect().toList == List((5, -42)))
      }
  }

  val sql_mcio =
    s"SELECT SUM(${SampleColB} - ${SampleColA}), SUM(${SampleColA} + ${SampleColB}) FROM nums"
  "Support multi-column inputs and outputs" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._

    sparkSession.sql(sql_mcio).ensureCEvaluating().debugSqlHere { ds =>
      assert(ds.as[(Double, Double)].collect().toList == List(-42.0 -> 82.0))
    }
  }

  "Support multi-column inputs and outputs with a .limit()" in withSparkSession2(configuration) {
    val sql_pairwise =
      s"SELECT ${SampleColA} + ${SampleColB}, ${SampleColA} - ${SampleColB} FROM nums"
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._

      sparkSession.sql(sql_pairwise).ensureCEvaluating().debugSqlHere { ds =>
        expect(
          ds.as[(Double, Double)].collect().toList.sortBy(_._1) == List[(Double, Double)](
            5.0 -> -1,
            58.0 -> 46.0,
            3.0 -> -1.0,
            9.0 -> -1.0,
            7.0 -> -1.0
          )
            .sortBy(_._1)
        )
      }
  }

  "Different multi-column expressions can be evaluated" - {
    val sql1 = s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA}) FROM nums"
    s"Multi-column: ${sql1}" in withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql1).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double, Double)].collect().toList == List(24.8 -> 62.0))
      }
    }

    val sql2 =
      s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA} - 1), ${SampleColA} / 2 FROM nums GROUP BY (${SampleColA} / 2)"

    s"Group by is possible with ${sql2}" ignore withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql2).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double, Double, Double)].collect().toList == Nil)
      }
    }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def ensureCEvaluating(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("CEvaluation"))
      dataSet
    }
    def debugSqlHere[V](f: Dataset[T] => V): V = {
      withClue(dataSet.queryExecution.executedPlan.toString()) {
        f(dataSet)
      }
    }
  }

}
