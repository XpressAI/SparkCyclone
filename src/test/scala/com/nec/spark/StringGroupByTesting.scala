package com.nec.spark

import com.eed3si9n.expecty.Expecty.assert
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SampleTestData.{SampleStrCsv, SampleStrCsv2}
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

final case class StringGroupByTesting(isVe: Boolean) extends Testing {
  override type Result = (String, Double)
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

    VERewriteStrategy.failFast = true
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

    sparkSession.read
      .schema(StructType(Seq(StructField("value", StringType), StructField("o", DoubleType))))
      .option("header", "false")
      .csv(SampleStrCsv2.toAbsolutePath.toString)
      .createOrReplaceTempView("sample_tbl")

    // note the filtering is probably done in the JVM here
    val ds = sparkSession
      .sql(
        "SELECT (CASE WHEN value = 'abc' THEN 'Y' ELSE 'N' END), sum(o) FROM sample_tbl WHERE value IN ('abc', 'def') group by value"
      )
      .as[Result]

//    val planString = ds.queryExecution.executedPlan.toString()
//    List("NewCEvaluation").foreach { expStr =>
//      assert(planString.contains(expStr), s"Expected the plan to contain '$expStr', but it didn't")
//    }

    ds
  }
  override def verifyResult(dataset: List[Result]): Unit = {
    val expected = List[Result]("Y" -> 3.0, "N" -> 1.0)
    assert(dataset == expected)
  }
  override def testingTarget: Testing.TestingTarget =
    if (isVe) TestingTarget.VectorEngine else TestingTarget.CMake
}
