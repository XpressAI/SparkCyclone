package com.nec.spark.planning

import com.eed3si9n.expecty.Expecty.assert
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.agile.CFunctionGeneration.GroupBeforeSort
import com.nec.spark.{AuroraSqlPlugin, SparkAdditions}
import com.nec.testing.SampleSource._
import com.nec.testing.Testing.DataSize.{BenchmarkSize, SanityCheckSize}
import com.nec.testing.Testing.TestingTarget
import com.nec.testing.{SampleSource, Testing}
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfter, Inside}

object GroupBySumPlanSpec {

  final case class GroupBySumPlanTesting(testingTarget: TestingTarget, source: SampleSource)
    extends Testing {
    type Result = (Double, Double)
    override def verifyResult(result: List[Result]): Unit = {
      assert(result.sortBy(_._1) == List((10.0, 25.0), (20.0, 0.0), (40.0, 42.0)))
    }
    override def prepareSession(): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config(
          key = "spark.plugins",
          value = if (testingTarget.isVE) classOf[AuroraSqlPlugin].getCanonicalName else ""
        )
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          if (testingTarget.isCMake)
            sse.injectPlannerStrategy(sparkSession => VERewriteStrategy(CNativeEvaluator))
        )
        .getOrCreate()
    }

    override def prepareInput(
      sparkSession: SparkSession,
      dataSize: Testing.DataSize
    ): Dataset[Result] = {
      import sparkSession.sqlContext.implicits._
      dataSize match {
        case SanityCheckSize =>
          List[(Double, Double)](
            (10.0, 5.0),
            (10.0, 20.0),
            (20.0, 1.0),
            (40.0, 42.0),
            (20.0, -1.0)
          ).toDS
            .withColumnRenamed("_1", SampleColA)
            .withColumnRenamed("_2", SampleColB)
            .createOrReplaceTempView(SampleSource.SharedName)

        case BenchmarkSize => source.generate(sparkSession, dataSize)
      }

      val ds = sparkSession
        .sql(s"SELECT ${SampleColA}, sum(${SampleColB}) from ${SharedName} group by ${SampleColA}")
        .as[(Double, Double)]

      if (testingTarget.isNative) {
        assert(
          ds.queryExecution.executedPlan.toString().contains(GroupBeforeSort),
          "Native execution should have C code from the group-by generator"
        )
      }

      ds
    }
    override def cleanUp(sparkSession: SparkSession): Unit = {
      sparkSession.close()
    }

  }

  final case class GroupBySumPlainSparkTesting(source: SampleSource) extends Testing {
    type Result = (Double, Double)
    override def verifyResult(data: List[Result]): Unit = {
      assert(data.sortBy(_._1) == List((10.0, 25.0), (20.0, 0.0), (40.0, 42.0)))
    }
    override def prepareInput(
      sparkSession: SparkSession,
      dataSize: Testing.DataSize
    ): Dataset[(Double, Double)] = {
      import sparkSession.sqlContext.implicits._
      dataSize match {
        case SanityCheckSize =>
          List[(Double, Double)](
            (10.0, 5.0),
            (10.0, 20.0),
            (20.0, 1.0),
            (40.0, 42.0),
            (20.0, -1.0)
          ).toDS
            .withColumnRenamed("_1", SampleColA)
            .withColumnRenamed("_2", SampleColB)
            .createOrReplaceTempView(SampleSource.SharedName)

        case BenchmarkSize => source.generate(sparkSession, dataSize)
      }

      val ds = sparkSession
        .sql(s"SELECT ${SampleColA}, sum(${SampleColB}) from ${SharedName} group by ${SampleColA}")
        .as[(Double, Double)]

      ds
    }
    override def prepareSession(): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .getOrCreate()
    }
    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def testingTarget: Testing.TestingTarget = TestingTarget.PlainSpark
  }

  val OurTesting: List[Testing] = List(
    GroupBySumPlanTesting(TestingTarget.VectorEngine, CSV),
    GroupBySumPlanTesting(TestingTarget.CMake, CSV),
    GroupBySumPlanTesting(TestingTarget.PlainSpark, CSV),
    GroupBySumPlanTesting(TestingTarget.VectorEngine, Parquet),
    GroupBySumPlanTesting(TestingTarget.CMake, Parquet),
    GroupBySumPlanTesting(TestingTarget.PlainSpark, Parquet),
    GroupBySumPlainSparkTesting(CSV),
    GroupBySumPlainSparkTesting(Parquet)
  )
}

final class GroupBySumPlanSpec
  extends AnyFreeSpec
  with BenchTestAdditions
  with BeforeAndAfter
  with SparkAdditions
  with Inside
  with Matchers {
  GroupBySumPlanSpec.OurTesting.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)

  "GroupBySum plan should be rewritten by VERewriteStrategy" in withSparkSession2(
    _.withExtensions(sse =>
      sse.injectPlannerStrategy(sparkSession => new VERewriteStrategy(CNativeEvaluator))
    )
  ) { sparkSession =>
    import sparkSession.implicits._

    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    val plan = sparkSession
      .sql("SELECT nums._2, SUM(nums._1) FROM nums GROUP BY nums._2")
      .queryExecution
      .executedPlan

    assert(plan.isInstanceOf[NewCEvaluationPlan])
  }

}
