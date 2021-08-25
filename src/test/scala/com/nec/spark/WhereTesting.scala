package com.nec.spark

import com.eed3si9n.expecty.Expecty.assert
import com.nec.cmake.ReadFullCSVSpec.SampleRow
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SampleTestData.SampleCSV
import com.nec.spark.SampleTestData.SampleTXT
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

final case class WhereTesting(isVe: Boolean) extends Testing {
  override type Result = Double
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
          sse.injectPlannerStrategy(sparkSession =>
            VERewriteStrategy(sparkSession, CNativeEvaluator)
          )
        )
        .config(sparkConf)
        .getOrCreate()
  }
  override def prepareInput(
    sparkSession: SparkSession,
    dataSize: Testing.DataSize
  ): Dataset[Double] = {
    import sparkSession.implicits._

    val schema = StructType(
      Array(
        StructField("a", DoubleType),
        StructField("b", DoubleType),
        StructField("c", DoubleType)
      )
    )

    sparkSession.sqlContext.read
      .schema(schema)
      .option("header", "true")
      .csv(SampleTestData.SampleCSV.toString)
      .as[SampleRow]
      .createOrReplaceTempView("sample_tbl")

    val ds = sparkSession.sql("SELECT SUM(b) FROM sample_tbl WHERE a < 4").as[Double]

    val planString = ds.queryExecution.executedPlan.toString()
    assert(
      planString.contains("CEvaluation"),
      s"Expected the plan to contain 'CEvaluation', but it didn't"
    )
    assert(
      planString.contains("PushedFilters: []"),
      s"Expected the plan to contain no pushed filters, but it did"
    )

    assert(!planString.contains("Filter ("), "Expected Filter not to be there any more")

    ds
  }
  override def verifyResult(dataset: List[Double]): Unit = {
    val expected = List[Double](12d)
    assert(dataset == expected)
  }
  override def testingTarget: Testing.TestingTarget =
    if (isVe) TestingTarget.VectorEngine else TestingTarget.CMake
}
