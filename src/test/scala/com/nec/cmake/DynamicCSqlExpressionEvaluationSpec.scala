package com.nec.cmake

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec.configuration
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.SampleSource
import com.nec.testing.SampleSource.{
  makeCsvNumsMultiColumn,
  makeCsvNumsMultiColumnJoin,
  SampleColA,
  SampleColB
}
import com.nec.testing.Testing.DataSize.SanityCheckSize

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object DynamicCSqlExpressionEvaluationSpec {

  def configuration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession => new VERewriteStrategy(CNativeEvaluator))
      )
  }

}

final class DynamicCSqlExpressionEvaluationSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "Different single-column expressions can be evaluated" - {
    List(
      s"SELECT SUM(${SampleColA}) FROM nums" -> 90.0d,
      s"SELECT SUM(${SampleColA} - 1) FROM nums" -> 81.0d,
      /** The below are ignored for now */
      s"SELECT AVG(${SampleColA}) FROM nums" -> 10d,
      s"SELECT AVG(2 * ${SampleColA}) FROM nums" -> 20d,
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
      assert(
        ds.as[Option[Double]].collect().toList.sorted == List[Option[Double]](
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          Some(3),
          Some(5),
          Some(7),
          Some(9),
          Some(58)
        ).sorted
      )
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
  "Support multi-column outputs, order reversed" in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      sparkSession.sql(sql_mci_2).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[Double].collect().toList == List(-42.0))
      }
  }

  val sql_cnt = s"SELECT COUNT(*) FROM nums"
  "Support count" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    sparkSession.sql(sql_cnt).ensureCEvaluating().debugSqlHere { ds =>
      assert(ds.as[Long].collect().toList == List(13))
    }
  }

  val sql_cnt_multiple_ops = s"SELECT COUNT(*), SUM(${SampleColB} - ${SampleColA}) FROM nums"
  "Support count with other operations in the same query" in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      sparkSession.sql(sql_cnt_multiple_ops).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Long, Double)].collect().toList == List((13, -42)))
      }
  }

  val sql_select_sort = s"SELECT ${SampleColA}, ${SampleColB} FROM nums ORDER BY ${SampleColB}"
  "Support order by with select" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    sparkSession.sql(sql_select_sort).ensureSortPlanEvaluated().debugSqlHere { ds =>
      val a = ds.as[(Option[Double], Option[Double])].collect().toList
      val b = List(
        (Some(4.0), None),
        (Some(2.0), None),
        (None, None),
        (Some(2.0), None),
        (None, None),
        (Some(20.0), None),
        (Some(1.0), Some(2.0)),
        (Some(2.0), Some(3.0)),
        (None, Some(3.0)),
        (Some(3.0), Some(4.0)),
        (Some(4.0), Some(5.0)),
        (None, Some(5.0)),
        (Some(52.0), Some(6.0))
      )
      assert(a == b)
    }
  }

  val sql_select_sort2 =
    s"SELECT ${SampleColA}, ${SampleColB}, (${SampleColA} + ${SampleColB}) FROM nums ORDER BY ${SampleColB}"
  "Support order by with select with sum" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    sparkSession.sql(sql_select_sort2).ensureCEvaluating().debugSqlHere { ds =>
      assert(
        ds.as[(Option[Double], Option[Double], Option[Double])].collect().toList == List(
          (Some(4.0), None, None),
          (Some(2.0), None, None),
          (None, None, None),
          (Some(2.0), None, None),
          (None, None, None),
          (Some(20.0), None, None),
          (Some(1.0), Some(2.0), Some(3.0)),
          (Some(2.0), Some(3.0), Some(5.0)),
          (None, Some(3.0), None),
          (Some(3.0), Some(4.0), Some(7.0)),
          (Some(4.0), Some(5.0), Some(9.0)),
          (None, Some(5.0), None),
          (Some(52.0), Some(6.0), Some(58.0))
        )
      )
    }
  }

  val sql_mcio =
    s"SELECT SUM(${SampleColB} - ${SampleColA}), SUM(${SampleColA} + ${SampleColB}) FROM nums"
  "Support multi-column inputs and inputs" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._

    sparkSession.sql(sql_mcio).ensureCEvaluating().debugSqlHere { ds =>
      assert(
        ds.as[(Option[Double], Option[Double])].collect().toList == List((Some(-42.0), Some(82.0)))
      )
    }
  }

  val sql_join =
    s"SELECT nums.${SampleColB}, nums2.${SampleColB} FROM nums JOIN nums2 ON nums.${SampleColA} = nums2.${SampleColA}"
  "Support INNER EQUAL JOIN" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumnJoin(sparkSession)
    import sparkSession.implicits._

    sparkSession.sql(sql_join).ensureJoinPlanEvaluated().debugSqlHere { ds =>
      ds.as[(Option[Double], Option[Double])]
        .collect()
        .toList should contain theSameElementsAs List(
        (Some(2.0), Some(41.0)),
        (None, Some(44.0)),
        (None, Some(44.0)),
        (Some(3.0), Some(44.0)),
        (Some(6.0), Some(61.0)),
        (Some(5.0), None),
        (None, None),
        (None, None),
        (None, None),
        (Some(3.0), None),
        (Some(4.0), None),
        (None, Some(32.0))
      )

    }
  }

  val sql_multi_join =
    s"SELECT nums.${SampleColA},nums2.${SampleColA}, nums.${SampleColB}, nums2.${SampleColB}," +
      s"nums3.${SampleColA}, nums4.${SampleColA}, nums3.${SampleColB}, nums4.${SampleColB}" +
      s" FROM nums JOIN nums2 ON nums.${SampleColA} = nums2.${SampleColA} JOIN nums AS nums3 " +
      s"ON nums.${SampleColA} = nums3.${SampleColA} JOIN nums2 AS nums4 ON nums.${SampleColA} = nums4.${SampleColA}"
  "Support multiple inner join operations" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumnJoin(sparkSession)
    import sparkSession.implicits._

    sparkSession.sql(sql_multi_join).ensureJoinPlanEvaluated().debugSqlHere { ds =>
      ds.as[
        (
          Option[Double],
          Option[Double],
          Option[Double],
          Option[Double],
          Option[Double],
          Option[Double],
          Option[Double],
          Option[Double]
        )
      ].collect()
        .toList should contain theSameElementsAs
        List(
          (
            Some(1.0),
            Some(1.0),
            Some(2.0),
            Some(41.0),
            Some(1.0),
            Some(1.0),
            Some(2.0),
            Some(41.0)
          ),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), Some(3.0), None),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), Some(3.0), Some(44.0)),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), Some(3.0), None),
          (Some(2.0), Some(2.0), None, Some(44.0), Some(2.0), Some(2.0), Some(3.0), Some(44.0)),
          (Some(2.0), Some(2.0), Some(3.0), Some(44.0), Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), Some(3.0), Some(44.0), Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), Some(3.0), Some(44.0), Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), Some(3.0), Some(44.0), Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), Some(3.0), Some(44.0), Some(2.0), Some(2.0), Some(3.0), None),
          (
            Some(2.0),
            Some(2.0),
            Some(3.0),
            Some(44.0),
            Some(2.0),
            Some(2.0),
            Some(3.0),
            Some(44.0)
          ),
          (
            Some(52.0),
            Some(52.0),
            Some(6.0),
            Some(61.0),
            Some(52.0),
            Some(52.0),
            Some(6.0),
            Some(61.0)
          ),
          (Some(4.0), Some(4.0), Some(5.0), None, Some(4.0), Some(4.0), Some(5.0), None),
          (Some(4.0), Some(4.0), Some(5.0), None, Some(4.0), Some(4.0), None, None),
          (Some(4.0), Some(4.0), None, None, Some(4.0), Some(4.0), Some(5.0), None),
          (Some(4.0), Some(4.0), None, None, Some(4.0), Some(4.0), None, None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), Some(3.0), None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), Some(3.0), Some(44.0)),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), Some(3.0), None),
          (Some(2.0), Some(2.0), None, None, Some(2.0), Some(2.0), Some(3.0), Some(44.0)),
          (Some(2.0), Some(2.0), Some(3.0), None, Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), Some(3.0), None, Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), Some(3.0), None, Some(2.0), Some(2.0), None, None),
          (Some(2.0), Some(2.0), Some(3.0), None, Some(2.0), Some(2.0), None, Some(44.0)),
          (Some(2.0), Some(2.0), Some(3.0), None, Some(2.0), Some(2.0), Some(3.0), None),
          (Some(2.0), Some(2.0), Some(3.0), None, Some(2.0), Some(2.0), Some(3.0), Some(44.0)),
          (Some(3.0), Some(3.0), Some(4.0), None, Some(3.0), Some(3.0), Some(4.0), None),
          (Some(20.0), Some(20.0), None, Some(32.0), Some(20.0), Some(20.0), None, Some(32.0))
        )
    }
  }

  val sql_join_key_select =
    s"SELECT nums.${SampleColA},nums2.${SampleColA}, nums.${SampleColB}, nums2.${SampleColB} FROM nums JOIN nums2 ON nums.${SampleColA} = nums2.${SampleColA}"
  "Support INNER EQUAL JOIN with selection of join key" in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumnJoin(sparkSession)
      import sparkSession.implicits._

      sparkSession.sql(sql_join_key_select).ensureJoinPlanEvaluated().debugSqlHere { ds =>
        ds.as[(Option[Double], Option[Double], Option[Double], Option[Double])]
          .collect()
          .toList should contain theSameElementsAs
          List(
            (Some(1.0), Some(1.0), Some(2.0), Some(41.0)),
            (Some(2.0), Some(2.0), None, Some(44.0)),
            (Some(2.0), Some(2.0), None, Some(44.0)),
            (Some(2.0), Some(2.0), Some(3.0), Some(44.0)),
            (Some(52.0), Some(52.0), Some(6.0), Some(61.0)),
            (Some(4.0), Some(4.0), Some(5.0), None),
            (Some(4.0), Some(4.0), None, None),
            (Some(2.0), Some(2.0), None, None),
            (Some(2.0), Some(2.0), None, None),
            (Some(2.0), Some(2.0), Some(3.0), None),
            (Some(3.0), Some(3.0), Some(4.0), None),
            (Some(20.0), Some(20.0), None, Some(32.0))
          )

      }
  }

  val sql_join_self =
    s"SELECT nums.${SampleColA}, nums.${SampleColB} FROM nums JOIN nums as nums1 ON nums.${SampleColA} = nums1.${SampleColA}"
  "Support INNER EQUAL SELF JOIN " in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumnJoin(sparkSession)
    import sparkSession.implicits._
    val d = sparkSession.sql(sql_join_self)
    println(d.queryExecution.executedPlan)
    d.ensureJoinPlanEvaluated().debugSqlHere { ds =>
      ds.as[(Option[Double], Option[Double])].collect().toList should contain theSameElementsAs
        List(
          (Some(2.0), Some(3.0)),
          (Some(2.0), Some(3.0)),
          (Some(2.0), Some(3.0)),
          (Some(52.0), Some(6.0)),
          (Some(4.0), None),
          (Some(4.0), None),
          (Some(2.0), None),
          (Some(2.0), None),
          (Some(2.0), None),
          (Some(1.0), Some(2.0)),
          (Some(4.0), Some(5.0)),
          (Some(4.0), Some(5.0)),
          (Some(2.0), None),
          (Some(2.0), None),
          (Some(2.0), None),
          (Some(3.0), Some(4.0)),
          (Some(20.0), None)
        )

    }
  }

  "Support multi-column inputs and outputs with a .limit()" in withSparkSession2(configuration) {
    val sql_pairwise =
      s"SELECT ${SampleColA} + ${SampleColB}, ${SampleColA} - ${SampleColB} FROM nums"
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._

      sparkSession.sql(sql_pairwise).ensureCEvaluating().debugSqlHere { ds =>
        assert(
          ds.as[(Option[Double], Option[Double])].collect().toList == List(
            (Some(5.0), Some(-1.0)),
            (Some(58.0), Some(46.0)),
            (None, None),
            (None, None),
            (None, None),
            (Some(3.0), Some(-1.0)),
            (Some(9.0), Some(-1.0)),
            (None, None),
            (None, None),
            (None, None),
            (Some(7.0), Some(-1.0)),
            (None, None),
            (None, None)
          )
        )
      }
  }

  "Different multi-column expressions can be evaluated" - {
    val sql1 = s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA}) FROM nums"
    s"Multi-column: ${sql1}" in withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql1).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double, Double)].collect().toList == List((20.0, 90.0)))
      }
    }

    val sql2 =
      s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA} - 1), ${SampleColA} / 2 FROM nums GROUP BY (${SampleColA})"

    s"Group by is possible with ${sql2}" ignore withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql2).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double, Double, Double)].collect().toList == Nil)
      }
    }

    val sql3 = s"SELECT ${SampleColA}, SUM(${SampleColB}) as y FROM nums GROUP BY ${SampleColA}"

    s"Simple Group by is possible with ${sql3}" in withSparkSession2(configuration) {
      sparkSession =>
        SampleSource.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._

        sparkSession.sql(sql3).ensureGroupBySumPlanEvaluated().debugSqlHere { ds =>
          assert(
            ds.as[(Option[Double], Option[Double])].collect().toList.sorted ==
              List(
                (Some(0.0), Some(8.0)),
                (Some(1.0), Some(2.0)),
                (Some(2.0), Some(3.0)),
                (Some(3.0), Some(4.0)),
                (Some(4.0), Some(5.0)),
                (Some(20.0), Some(0.0)),
                (Some(52.0), Some(6.0))
              )
          )
        }
    }

    /*
    val sql4 = s"SELECT SUM(${SampleColB}) as y FROM nums GROUP BY ${SampleColA} HAVING y > 3"
    s"Simple filtering is possible with ${sql4}" in withSparkSession2(DynamicVeSqlExpressionEvaluationSpec.configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql4).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double)].collect().toList == List(15.0))
      }
    }
     */

    val sql5 = s"SELECT CORR(${SampleColA}, ${SampleColB}) as c FROM nums"
    s"Corr function is possible with ${sql5}" in withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql5).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double)].collect().toList == List(0.7418736765817244))
      }
    }

    val sql6 = s"SELECT MAX(${SampleColA}) AS a, MIN(${SampleColB}) AS b FROM nums"
    s"MIN and MAX work with ${sql6}" in withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql6).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Double, Double)].collect().toList == List((52.0, 2.0)))
      }
    }

    val sql7 = s"SELECT ${SampleColA}, ${SampleColB} FROM nums ORDER BY ${SampleColB}"
    s"Ordering with a group by: ${sql7}" in withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql7).ensureSortPlanEvaluated().debugSqlHere { ds =>
        assert(
          ds.as[(Option[Double], Option[Double])].collect().toList == List(
            (Some(4.0), None),
            (Some(2.0), None),
            (None, None),
            (Some(2.0), None),
            (None, None),
            (Some(20.0), None),
            (Some(1.0), Some(2.0)),
            (Some(2.0), Some(3.0)),
            (None, Some(3.0)),
            (Some(3.0), Some(4.0)),
            (Some(4.0), Some(5.0)),
            (None, Some(5.0)),
            (Some(52.0), Some(6.0))
          )
        )
      }
    }

    val sql8 =
      s"SELECT ${SampleColA}, SUM(${SampleColB}) AS y, MAX(${SampleColB}), MIN(${SampleColB}) FROM nums GROUP BY ${SampleColA} ORDER BY y"
    s"Ordering with a group by: ${sql8}" in withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql8).ensureSortPlanEvaluated().debugSqlHere { ds =>
        assert(
          ds.as[(Option[Double], Option[Double], Option[Double], Option[Double])]
            .collect()
            .toList == List(
            (Some(20.0), None, None, None),
            (Some(1.0), Some(2.0), Some(2.0), Some(2.0)),
            (Some(2.0), Some(3.0), Some(3.0), Some(3.0)),
            (Some(3.0), Some(4.0), Some(4.0), Some(4.0)),
            (Some(4.0), Some(5.0), Some(5.0), Some(5.0)),
            (Some(52.0), Some(6.0), Some(6.0), Some(6.0)),
            (None, Some(8.0), Some(5.0), Some(3.0))
          )
        )
      }
    }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def ensureCEvaluating(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("CEvaluation"))
      dataSet
    }

    def ensureGroupBySumPlanEvaluated(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("SimpleGroupBySumPlan"))
      dataSet
    }

    def ensureSortPlanEvaluated(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("SimpleSortPlan"))
      dataSet
    }

    def ensureJoinPlanEvaluated(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("GeneratedJoinPlan"))
      dataSet
    }

    def debugSqlHere[V](f: Dataset[T] => V): V = {
      withClue(dataSet.queryExecution.executedPlan.toString()) {
        f(dataSet)
      }
    }
  }

}
