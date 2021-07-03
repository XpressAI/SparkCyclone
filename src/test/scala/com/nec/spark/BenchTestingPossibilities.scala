package com.nec.spark
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec.cNativeEvaluator
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.planning.simplesum.JoinPlanSpec
import com.nec.testing.Testing
import com.nec.testing.Testing.DataSize
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS

object BenchTestingPossibilities {

  final case class SimpleSql(
    sql: String,
    expectedResult: Double,
    source: Source,
    testingTarget: TestingTarget
  ) extends Testing {
    override def benchmark(sparkSession: SparkSession): Unit = {
      val result = sparkSession.sql(sql)
      println(result.queryExecution.executedPlan)
      result.collect()
    }
    override def prepareSession(dataSize: DataSize): SparkSession = {
      val sparkConf = new SparkConf(loadDefaults = true)
      val sess = testingTarget match {
        case TestingTarget.Rapids =>
          SparkSession
            .builder()
            .appName(name.value)
            .master("local[*]")
            .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
            .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
            .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")
            .config(key = "spark.ui.enabled", value = false)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.VectorEngine =>
          LocalVeoExtension._enabled = true
          SparkSession
            .builder()
            .master("local[*]")
            .appName(name.value)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.PlainSpark =>
          SparkSession
            .builder()
            .master("local[*]")
            .appName(name.value)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.CMake =>
          SparkSession
            .builder()
            .master("local[*]")
            .appName(name.value)
            .withExtensions(sse =>
              sse.injectPlannerStrategy(sparkSession =>
                new VERewriteStrategy(sparkSession, cNativeEvaluator)
              )
            )
            .config(CODEGEN_FALLBACK.key, value = false)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
      }

      source.generate(sess, dataSize)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult)
    }
  }

  val possibilities: List[Testing] =
    List(
      for {
        source <- List(Source.CSV, Source.Parquet)
        testingTarget <- List(
          TestingTarget.VectorEngine,
          TestingTarget.PlainSpark,
          TestingTarget.Rapids,
          TestingTarget.CMake
        )
      } yield SimpleSql(
        sql = s"SELECT SUM(value) FROM nums",
        expectedResult = 123,
        source = source,
        testingTarget = testingTarget
      ),
      JoinPlanSpec.OurTesting
    ).flatten

  trait BenchTestAdditions { this: AnyFreeSpec =>
    def runTestCase(testing: Testing): Unit = {
      testing.name.value in {
        val sparkSession = testing.prepareSession(dataSize = DataSize.SanityCheckSize)
        try testing.verify(sparkSession)
        finally testing.cleanUp(sparkSession)
      }
    }
  }
}

final class BenchTestingPossibilities extends AnyFreeSpec with BenchTestAdditions {

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)

}
