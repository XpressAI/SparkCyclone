package com.nec.spark.agile

import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.cmake.CMakeBuilder
import com.nec.spark.agile.PairwiseAdditionOffHeap.OffHeapPairwiseSummer
import com.nec.spark.planning.{AddPlanExtractor, ArrowVeoAvgPlanExtractor, ArrowVeoSumPlanExtractor, SparkSqlPlanExtension, SumPlanExtractor}
import com.nec.spark.Aurora4SparkDriver
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.SampleTestData.SampleMultiColumnCSV
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class SparkSanityTests
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "It is not launched by default" in withSpark(identity) { _ =>
    assert(!Aurora4SparkDriver.launched, "Expect the driver to have not been launched")
    assert(
      !Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.isEmpty,
      "Expect the executor plugin to have not been launched"
    )
  }

  "We can run a Spark-SQL job for a sanity test" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
    assert(result == List(3))
  }

  "From the execution plan, we get the inputted numbers" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
      .toDS()
      .createOrReplaceTempView("nums")

    val executionPlan =
      sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].executionPlan

    assert(
      executionPlan.getClass.getCanonicalName
        == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
    )

    assert(
      SumPlanExtractor
        .matchPlan(executionPlan)
        .contains(List(1, 2, 3))
    )
  }

  "We Pairwise-add off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      AddPlanExtractor
        .matchAddPairwisePlan(sparkPlan, new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString))
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val csvSchema = StructType(
      Seq(
        StructField("a", DoubleType, nullable = false),
        StructField("b", DoubleType, nullable = false)
      )
    )
    val sumDataSet2 = sparkSession.read
      .format("csv")
      .schema(csvSchema)
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .selectExpr("a + b")
      .as[Double]
      .debugConditionally()

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(3, 5, 7, 9, 58))
  }

  "We Pairwise-add Parquet off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      AddPlanExtractor
        .matchAddPairwisePlan(sparkPlan, new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString))
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val sumDataSet2 = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("a + b")
      .as[Double]
      .debugConditionally()

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(3, 5, 7, 9, 58))
  }


  "We handle single column average with specific plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      ArrowVeoAvgPlanExtractor
        .matchPlan(sparkPlan, new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString))
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val sumDataSet2 = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("AVG(a)")
      .as[Double]
      .debugConditionally()

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(12.4))
  }

  "We handle single column sum with specific plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      ArrowVeoSumPlanExtractor
        .matchPlan(sparkPlan, new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString))
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val sumDataSet2 = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a)")
      .as[Double]
      .debugConditionally()

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(62.0))
  }

}
