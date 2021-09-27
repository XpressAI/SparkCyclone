package com.nec.cmake

import com.eed3si9n.expecty.Expecty.expect
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.SampleSource
import com.nec.testing.SampleSource.{SampleColA, SampleColB, SampleColC, SampleColD, makeCsvNumsMultiColumn, makeCsvNumsMultiColumnJoin}
import com.nec.testing.Testing.DataSize.SanityCheckSize
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.{Dataset, SparkSession}

object DynamicCSqlExpressionEvaluationSpec {

  val DefaultConfiguration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession => new VERewriteStrategy(CNativeEvaluator))
      )
  }

}

class DynamicCSqlExpressionEvaluationSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with BeforeAndAfterAll
  with SparkAdditions
  with Matchers
  with LazyLogging {

  def configuration: SparkSession.Builder => SparkSession.Builder =
    DynamicCSqlExpressionEvaluationSpec.DefaultConfiguration

  "Different single-column expressions can be evaluated" - {
    List(
      s"SELECT SUM(${SampleColA}) FROM nums" -> 90.0d,
      s"SELECT SUM(${SampleColD}) FROM nums" -> 165.0,
      s"SELECT SUM(${SampleColA} - 1) FROM nums" -> 81.0d,
      /** The below are ignored for now */
      s"SELECT AVG(${SampleColA}) FROM nums" -> 10d,
      s"SELECT AVG(2 * ${SampleColA}) FROM nums" -> 20d,
      s"SELECT AVG( 2 * ${SampleColD}) FROM nums" -> 30.0d,
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

  val sql_pairwise_short = s"SELECT ${SampleColD} + ${SampleColD} FROM nums"
  "Support pairwise addition with shorts" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumn(sparkSession)
    import sparkSession.implicits._
    sparkSession.sql(sql_pairwise_short).debugSqlHere { ds =>
      assert(
        ds.as[Option[Short]].collect().toList.sorted == List(
          None, None, Some(2), Some(4), Some(6), Some(8), Some(16), Some(18), Some(22), Some(24), Some(46), Some(84), Some(100)
        )
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

  val sql_cnt_multiple_ops = s"SELECT COUNT(*), SUM(${SampleColB} - ${SampleColA}) FROM nums"
  "Support count with other operations in the same query" in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)
      import sparkSession.implicits._
      sparkSession.sql(sql_cnt_multiple_ops).ensureCEvaluating().debugSqlHere { ds =>
        assert(ds.as[(Long, Double)].collect().toList == List((13, -42)))
      }
  }

  val sql_select_sort2 =
    s"SELECT ${SampleColA}, ${SampleColB}, (${SampleColA} + ${SampleColB}) FROM nums ORDER BY ${SampleColB}"
  "Support order by with select with addition" ignore withSparkSession2(configuration) {
    sparkSession =>
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

  val multisort_sql =
    s"SELECT ${SampleColA}, ${SampleColB}, SUM(${SampleColC}) FROM nums GROUP BY ${SampleColA}, ${SampleColB}"
  "Support group by sum with multiple grouping columns" in withSparkSession2(configuration) {
    sparkSession =>
      makeCsvNumsMultiColumn(sparkSession)

      import sparkSession.implicits._
      //Results order here is different due to the fact that we sort the columns and seems that spark does not.
      sparkSession.sql(multisort_sql).debugSqlHere { ds =>
        ds.as[(Option[Double], Option[Double], Option[Double])]
          .collect()
          .toList should contain theSameElementsAs List(
          (None, None, Some(8.0)),
          (Some(4.0), Some(5.0), None),
          (Some(52.0), Some(6.0), None),
          (Some(4.0), None, Some(2.0)),
          (Some(2.0), Some(3.0), Some(4.0)),
          (None, Some(3.0), Some(1.0)),
          (Some(2.0), None, Some(2.0)),
          (Some(3.0), Some(4.0), Some(5.0)),
          (Some(1.0), Some(2.0), Some(8.0)),
          (Some(20.0), None, None),
          (None, Some(5.0), Some(2.0))
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
      val result = ds
        .as[(Option[Double], Option[Double])]
        .collect()
        .toList
        .sorted

      val expected = List(
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
      ).sorted

      assert(result == expected)

    }
  }

  val sql_join_outer_left =
    s"SELECT nums.${SampleColA}, nums2.${SampleColA} FROM nums LEFT JOIN nums2 on nums.${SampleColB} = nums2.${SampleColB}"
  "Support LEFT OUTER JOIN" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumnJoin(sparkSession)
    import sparkSession.implicits._

    sparkSession.sql(sql_join_outer_left).debugSqlHere { ds =>
      ds.as[(Option[Double], Option[Double])]
        .collect()
        .toList should contain theSameElementsAs List(
        (Some(2.0), None),
        (Some(52.0), None),
        (Some(4.0), None),
        (None, None),
        (Some(2.0), None),
        (Some(1.0), None),
        (Some(4.0), None),
        (None, None),
        (None, None),
        (Some(2.0), None),
        (Some(3.0), None),
        (None, None),
        (Some(20.0), None)
      )
    }
  }

  val sql_join_outer_right =
    s"SELECT nums.${SampleColA}, nums2.${SampleColA} FROM nums RIGHT JOIN nums2 on nums.${SampleColB} = nums2.${SampleColB}"
  "Support RIGHT OUTER JOIN" in withSparkSession2(configuration) { sparkSession =>
    makeCsvNumsMultiColumnJoin(sparkSession)
    import sparkSession.implicits._

    sparkSession.sql(sql_join_outer_right).debugSqlHere { ds =>
      val out = ds
        .as[(Option[Double], Option[Double])]
        .collect()
        .toList

      out should contain theSameElementsAs List(
        (None, Some(1.0)),
        (None, None),
        (Some(4.0), None),
        (None, None),
        (None, Some(2.0)),
        (None, Some(42.0)),
        (None, Some(12.0)),
        (None, Some(52.0)),
        (None, Some(4.0)),
        (None, None),
        (None, Some(2.0)),
        (None, Some(3.0)),
        (None, None),
        (None, Some(20.0))
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
    logger.info(d.queryExecution.executedPlan.toString())
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

      sparkSession.sql(sql_pairwise).debugSqlHere { ds =>
        val sortedOutput = ds.as[(Option[Double], Option[Double])].collect().toList.sorted
        val expected = List(
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
        ).sorted
        assert(sortedOutput == expected)
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
        ds.as[(Option[Double], Option[Double], Option[Double])].collect().toList should contain theSameElementsAs
          List((None,None,None), (Some(2.0),Some(0.0),Some(0.5)), (Some(8.0),Some(6.0),Some(2.0)), (Some(6.0),Some(2.0),Some(1.5)), (Some(4.0),Some(3.0),Some(1.0)), (Some(40.0),Some(19.0),Some(10.0)), (Some(104.0),Some(51.0),Some(26.0)))
      }
    }

    val sql3 = s"SELECT ${SampleColA}, SUM(${SampleColB}), SUM(${SampleColD}) FROM nums GROUP BY ${SampleColA}"

    s"Simple Group by is possible with ${sql3}" in withSparkSession2(configuration) {
      sparkSession =>
        SampleSource.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._

        sparkSession.sql(sql3).ensureNewCEvaluating().debugSqlHere { ds =>
          assert(
            ds.as[(Option[Double], Option[Double], Option[BigInt])].collect().toList.sorted ==
              List((None,Some(8.0),Some(60)), (Some(1.0),Some(2.0),None), (Some(2.0),Some(3.0),Some(32)), (Some(3.0),Some(4.0),Some(1)), (Some(4.0),Some(5.0),Some(46)), (Some(20.0),None,Some(3)), (Some(52.0),Some(6.0),Some(23)))
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
        val result = ds.as[(Double)].collect().toList
        println(ds.queryExecution.executedPlan)
        assert(result.size == 1)
        result.head shouldEqual (0.7418736765817244 +- 0.05)
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
    s"Ordering with a group by: ${sql7}" ignore withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql7).ensureNewCEvaluating().debugSqlHere { ds =>
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
    s"Ordering with a group by: ${sql8}" ignore withSparkSession2(configuration) { sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(sql8).ensureNewCEvaluating().debugSqlHere { ds =>
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

  val castSql =
    s"SELECT cast(${SampleColA} as bigint), cast(${SampleColA} as int), cast(${SampleColA} as double) FROM nums"
  s"Casting to various types works" in withSparkSession2(configuration) { sparkSession =>
    SampleSource.CSV.generate(sparkSession, SanityCheckSize)
    import sparkSession.implicits._

    sparkSession.sql(castSql).debugSqlHere { ds =>
      val result = ds
        .as[(Option[Long], Option[Int], Option[Double])]
        .collect()
        .toList
        .sorted

      val expected = List[(Option[Long], Option[Int], Option[Double])](
        (Some(2), Some(2), Some(2.0)),
        (Some(52), Some(52), Some(52.0)),
        (Some(4), Some(4), Some(4.0)),
        (None, None, None),
        (Some(2), Some(2), Some(2.0)),
        (Some(1), Some(1), Some(1.0)),
        (Some(4), Some(4), Some(4.0)),
        (None, None, None),
        (None, None, None),
        (Some(2), Some(2), Some(2.0)),
        (Some(3), Some(3), Some(3.0)),
        (None, None, None),
        (Some(20), Some(20), Some(20.0))
      ).sorted

      assert(result == expected)
    }
  }

  val innerCastSql =
    s"SELECT $SampleColB, SUM(cast($SampleColA AS double)) FROM nums GROUP BY $SampleColB"
  s"Casting inside a sum to various types works" in withSparkSession2(configuration) {
    sparkSession =>
      SampleSource.CSV.generate(sparkSession, SanityCheckSize)
      import sparkSession.implicits._

      sparkSession.sql(innerCastSql).debugSqlHere { ds =>
        ds.as[(Option[Double], Option[Double])]
          .collect()
          .toList should contain theSameElementsAs List(
          (None, Some(28.0)),
          (Some(2.0), Some(1.0)),
          (Some(3.0), Some(2.0)),
          (Some(4.0), Some(3.0)),
          (Some(5.0), Some(4.0)),
          (Some(6.0), Some(52.0))
        )

      }
  }

  s"Join query does cause compiler error" in withSparkSession2(configuration) { sparkSession =>
    SampleSource.makeConvertedParquetData(sparkSession)
    sparkSession
      .sql(
        "select simple_id from t1 join t2 on sample_id_c09c808524c21082de7576e875cab4fc==simple_id"
      )
      .ensureJoinPlanEvaluated()
      .debugSqlHere { ds =>
        println(ds.queryExecution.executedPlan)
        assert(ds.count() == 4)
      }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def ensureCEvaluating(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("CEvaluation"))
      dataSet
    }

    def ensureNewCEvaluating(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("NewCEvaluationPlan"))
      dataSet
    }

    def ensureJoinPlanEvaluated(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("GeneratedJoinPlan"))
      dataSet
    }

    def debugSqlHere[V](f: Dataset[T] => V): V = {
      try f(dataSet)
      catch {
        case e: Throwable =>
          logger.info(s"${dataSet.queryExecution.executedPlan}; ${e}", e)
          throw e
      }
    }
  }

}
