package com.nec.spark
import com.eed3si9n.expecty.Expecty.assert
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SampleTestData.SampleTXT
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS
import org.apache.spark.sql.types.{StringType, StructField, StructType}

final case class SubstringTesting(isVe: Boolean) extends Testing {
  override type Result = (String, Long, String)
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
      .schema(StructType(Seq(StructField("value", StringType), StructField("o", StringType))))
      .option("header", "false")
      .csv(SampleTXT.toAbsolutePath.toString)
      .createOrReplaceTempView("sample_tbl")

    val ds = sparkSession
      .sql(
        "SELECT SUBSTR(value, 1, 3) || o, LENGTH(value), SUBSTR(value, 1, LENGTH(value) - 2) FROM sample_tbl"
      )
      .as[Result]

    val planString = ds.queryExecution.executedPlan.toString()
    List("NewCEvaluation").foreach { expStr =>
      assert(planString.contains(expStr), s"Expected the plan to contain '$expStr', but it didn't")
    }

    ds
  }
  override def verifyResult(dataset: List[Result]): Unit = {
    val inputLines = List("This is", "some test", "of some stuff", "that always is")
    val o = List("a", "bb", "ccc", "dddd")
    val expected =
      inputLines.zip(o).map { case (str, o) =>
        (str.substring(1, 3), str.length.toLong, str.substring(1, str.length - 2))
      }
    assert(dataset == expected)
  }
  override def testingTarget: Testing.TestingTarget =
    if (isVe) TestingTarget.VectorEngine else TestingTarget.CMake
}
