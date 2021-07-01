package com.nec.spark.planning.simplesum
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.BenchTestingPossibilities.Testing
import com.nec.spark.cgescape.CodegenEscapeSpec.{makeCsvNums, makeParquetNums, makeParquetNumsLarge, makeCsvNumsLarge, makeMemoryNums}
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
import com.nec.spark.BenchTestingPossibilities.Testing.DataSize
import com.nec.spark.BenchTestingPossibilities.Testing.DataSize.{BenchmarkSize, SanityCheckSize}
import com.nec.spark.agile.CleanName
import com.nec.spark.agile.CleanName.RichStringClean

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

  sealed trait Source extends Serializable {
    def title: String
    def isColumnar: Boolean
    def generate(sparkSession: SparkSession, size: DataSize): Unit
  }

  object Source {
    case object CSV extends Source {
      override def isColumnar: Boolean = false
      override def generate(sparkSession: SparkSession, size: DataSize): Unit = {
        size match {
          case BenchmarkSize => makeCsvNumsLarge(sparkSession)
          case SanityCheckSize => makeCsvNums(sparkSession)
        }
      }

      override def title: String = "CSV"
    }
    case object Parquet extends Source {
      override def isColumnar: Boolean = true
      override def generate(sparkSession: SparkSession, size: DataSize): Unit = {
        size match {
          case BenchmarkSize => makeParquetNumsLarge(sparkSession)
          case SanityCheckSize => makeParquetNums(sparkSession)
        }
      }

      override def title: String = "Parquet"
    }
    case object InMemory extends Source {
      override def isColumnar: Boolean = true
      override def generate(sparkSession: SparkSession, size: DataSize): Unit =
        makeMemoryNums(sparkSession)
      override def title: String = "LocalTable"
    }

    val All: List[Source] = List(CSV, Parquet, InMemory)
  }

  // not used, need to reincorporate back somehow

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      System.err.println(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

  val OurTesting: List[Testing] = {
    for {
      sumMethod <- PureJvmBasedModes
      source <- Source.All
      sql = "SELECT SUM(value) FROM nums"
    } yield new Testing {
      override def name: CleanName = s"SimpleSum_${sumMethod.title}_${source}".clean
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
      override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
      override def requiresVe: Boolean = false
    }
  }
}

final class SimpleSumPlanTest extends AnyFreeSpec with BenchTestAdditions {

  SimpleSumPlanTest.OurTesting.foreach(runTestCase)

}
