package com.nec.spark.agile

import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.simplesum.SimpleSumPlan.SumMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.freespec.AnyFreeSpec
import com.eed3si9n.expecty.Expecty.assert
import com.nec.spark.planning.simplesum.SimpleSumPlan
import com.nec.testing.SampleSource
import com.nec.testing.SampleSource.SampleColA
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget

object SimpleSumPlanTest {
  val PureJvmBasedModes: List[SumMethod] = List(
    SumMethod.NonCodegen.RDDBased,
    SumMethod.CodegenBased.ArrowCodegenBased(ArrowSummer.JVMBased),
    SumMethod.CodegenBased.JvmIncremental
  )

  def X86AvailableModes(libPath: String): List[SumMethod] = List(
    SumMethod.CodegenBased.ArrowCodegenBased(ArrowSummer.CBased(libPath))
  )

  def VhAvailableModes(libPath: String): List[SumMethod] = List(
    SumMethod.CodegenBased.ArrowCodegenBased(ArrowSummer.VeoBased)
  )

  // not used, need to reincorporate back somehow

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      System.out.println(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

  final case class SimpleSumTesting(sumMethod: SumMethod, source: SampleSource, sql: String)
    extends Testing {
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      val ds = sparkSession.sql(sql).debugSqlHere.as[Double]
      val result = ds.collect().toList
      assert(result == List[Double](62))
    }
    override def benchmark(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      val ds = sparkSession.sql(sql).as[Double]
      ds.collect().toList
    }
    override def prepareSession(dataSize: Testing.DataSize): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      val ss = SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(SimpleSumPlan(planLater(child), sumMethod))
                  case _ => Nil
                }
            }
          )
        )
        .getOrCreate()

      source.generate(ss, dataSize)

      ss
    }
    override def testingTarget: Testing.TestingTarget = TestingTarget.PlainSpark
  }

  val OurTesting: List[Testing] = {
    for {
      sumMethod <- PureJvmBasedModes
      source <- SampleSource.All
      sql = s"SELECT SUM(${SampleColA}) FROM nums"
    } yield SimpleSumTesting(sumMethod, source, sql)
  }
}

final class SimpleSumPlanTest extends AnyFreeSpec with BenchTestAdditions {

  SimpleSumPlanTest.OurTesting.foreach(runTestCase)

}
