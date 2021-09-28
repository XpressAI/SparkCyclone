package com.nec.spark

import com.eed3si9n.expecty.Expecty.assert
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

final case class DateTesting(isVe: Boolean) extends Testing {
  override type Result = Int
  private val MasterName = "local[8]"

  override def prepareSession(): SparkSession = {
    val sparkConf = new SparkConf(loadDefaults = true)
      .set("nec.testing.target", testingTarget.label)
      .set("nec.testing.testing", this.toString)
      .set("spark.sql.codegen.comments", "true")
    val builder = SparkSession
      .builder()
      .master(MasterName)
      .appName(name.value)
      .config(CODEGEN_COMMENTS.key, value = true)
      .config(key = "spark.ui.enabled", value = false)
      .config(key = "spark.sql.csv.filterPushdown.enabled", value = false)

    if (isVe)
      builder
        .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
        .config(sparkConf)
        .getOrCreate()
    else
      builder
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession => VERewriteStrategy(CNativeEvaluator))
        )
        .config(sparkConf)
        .getOrCreate()
  }

  override def prepareInput(
    sparkSession: SparkSession,
    dataSize: Testing.DataSize
  ): Dataset[Result] = {
    import sparkSession.implicits._

    val schema = StructType(
      Array(StructField("l_shipdate", DateType), StructField("l_count", IntegerType))
    )

    sparkSession.sqlContext.read
      .schema(schema)
      .option("header", "true")
      .csv(SampleTestData.SampleDateCSV.toString)
      .createOrReplaceTempView("sample_tbl")

    val ds = sparkSession
      .sql("SELECT l_count FROM sample_tbl WHERE l_shipdate <= date_sub(date '1998-12-01', 3)")
      .as[Result]

//    val planString = ds.queryExecution.executedPlan.toString()
//    assert(
//      planString.contains("CEvaluation"),
//      s"Expected the plan to contain 'CEvaluation', but it didn't"
//    )
//
//    assert(
//      planString.contains("PushedFilters: []"),
//      s"Expected the plan to contain no pushed filters, but it did"
//    )
//    assert(!planString.contains("Filter ("), "Expected Filter not to be there any more")

    ds
  }

  override def verifyResult(dataset: List[Result]): Unit = {
    val expected = List[Int](9, 10, 11, 12, 13, 15)
    assert(dataset == expected)
  }

  override def testingTarget: Testing.TestingTarget =
    if (isVe) TestingTarget.VectorEngine else TestingTarget.CMake
}
