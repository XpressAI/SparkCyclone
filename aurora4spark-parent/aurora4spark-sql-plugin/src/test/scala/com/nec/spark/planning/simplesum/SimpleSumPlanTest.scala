package com.nec.spark.planning.simplesum
import com.nec.spark.SparkAdditions
import com.nec.spark.cgescape.CodegenEscapeSpec.makeCsvNums
import com.nec.spark.cgescape.CodegenEscapeSpec.makeMemoryNums
import com.nec.spark.cgescape.CodegenEscapeSpec.makeParquetNums
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.simplesum.SimpleSumPlan.SumMethod
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.PureJvmBasedModes
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

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
    def generate(sparkSession: SparkSession): Unit
  }

  object Source {
    case object CSV extends Source {
      override def isColumnar: Boolean = false
      override def generate(sparkSession: SparkSession): Unit = makeCsvNums(sparkSession)
      override def title: String = "CSV"
    }
    case object Parquet extends Source {
      override def isColumnar: Boolean = true
      override def generate(sparkSession: SparkSession): Unit = makeParquetNums(sparkSession)
      override def title: String = "Parquet"
    }
    case object InMemory extends Source {
      override def isColumnar: Boolean = true
      override def generate(sparkSession: SparkSession): Unit = makeMemoryNums(sparkSession)
      override def title: String = "LocalTable"
    }

    val All: List[Source] = List(CSV, Parquet, InMemory)
  }
}

final class SimpleSumPlanTest extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  PureJvmBasedModes.foreach { sumMethod =>
    s"${sumMethod.title}" - {
      Source.All.foreach { source =>
        s"${source.title}" in withSparkSession2(
          _.config(CODEGEN_FALLBACK.key, value = false)
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
        ) { sparkSession =>
          source.generate(sparkSession)
          val sql = "SELECT SUM(value) FROM nums"
          import sparkSession.implicits._
          val ds = sparkSession.sql(sql).debugSqlHere.as[Double]
          val result = ds.collect().toList
          assert(result == List[Double](62))
        }
      }
    }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      info(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

}
