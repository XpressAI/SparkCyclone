package com.nec.cmake

import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.spark.planning.AddPlanExtractor
import com.nec.spark.planning.SparkSqlPlanExtension
import com.nec.spark.SampleTestData.SampleMultiColumnCSV
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.ArrowAveragingPlan
import com.nec.spark.planning.ArrowSummingPlan
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.CBased
import com.nec.spark.planning.SingleColumnAvgPlanExtractor
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

final class PairwiseAdditionSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We Pairwise-add off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      AddPlanExtractor
        .matchAddPairwisePlan(
          sparkPlan,
          new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString)
        )
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
        .matchAddPairwisePlan(
          sparkPlan,
          new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString)
        )
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

}
