package com.nec.spark.planning

import com.eed3si9n.expecty.Expecty.assert
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.{AuroraSqlPlugin, SparkAdditions}
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod
import com.nec.testing.SampleSource.{CSV, Parquet, SampleColA, SampleColB, SharedName}
import com.nec.testing.Testing.DataSize.{BenchmarkSize, SanityCheckSize}
import com.nec.testing.Testing.TestingTarget
import com.nec.testing.{SampleSource, Testing}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.{BeforeAndAfter, Inside}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.{Dataset, SparkSession, Strategy}

object GroupBySumPlanSpec {

  final case class GroupBySumPlanTesting(groupByMethod: GroupByMethod, source: SampleSource)
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
      VERewriteStrategy._enabled = false
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
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(SimpleGroupBySumPlan(planLater(child), CNativeEvaluator, groupByMethod))
                  case _ => Nil
                }
              }
            }
          )
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

      import sparkSession.sqlContext.implicits._

      sparkSession
        .sql(s"SELECT ${SampleColA}, sum(${SampleColB}) from ${SharedName} group by ${SampleColA}")
        .as[(Double, Double)]
    }
    override def cleanUp(sparkSession: SparkSession): Unit = {
      sparkSession.close()
      VERewriteStrategy._enabled = true
    }
    override def testingTarget: Testing.TestingTarget = groupByMethod match {
      case GroupByMethod.VEBased       => TestingTarget.VectorEngine
      case GroupByMethod.JvmArrowBased => TestingTarget.PlainSpark
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
      import sparkSession.sqlContext.implicits._

      sparkSession
        .sql(s"SELECT ${SampleColA}, sum(${SampleColB}) from ${SharedName} group by ${SampleColA}")
        .as[(Double, Double)]
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
    GroupBySumPlanTesting(GroupByMethod.VEBased, CSV),
    GroupBySumPlanTesting(GroupByMethod.JvmArrowBased, CSV),
    GroupBySumPlainSparkTesting(CSV),
    GroupBySumPlanTesting(GroupByMethod.VEBased, Parquet),
    GroupBySumPlanTesting(GroupByMethod.JvmArrowBased, Parquet),
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

    inside(plan) { case SimpleGroupBySumPlan(LocalTableScanExec(output, rows), evaluator, method) =>
      method shouldBe GroupByMethod.VEBased
    }
  }

  "GroupBySum plan should correctly sum groups" in withSparkSession2(
    _.withExtensions(sse =>
      sse.injectPlannerStrategy(sparkSession => new VERewriteStrategy(CNativeEvaluator))
    )
  ) { sparkSession =>
    import sparkSession.implicits._

    Seq[(Double, Double)]((1, 5), (2, 1), (3, 9), (1, 9), (2, 78), (3, 91))
      .toDS()
      .createOrReplaceTempView("nums")

    val plan = sparkSession
      .sql("SELECT nums._2, SUM(nums._1) FROM nums GROUP BY nums._2")
      .as[(Double, Double)]

    plan.collect().toSeq shouldEqual Seq((1.0, 14.0), (2.0, 79.0), (3.0, 100.0))
  }
}
