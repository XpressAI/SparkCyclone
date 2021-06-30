package com.nec.spark.planning

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
import com.nec.spark.planning.SimpleSumPlanV2Test.BenchTestAdditions
import com.nec.spark.planning.SimpleSumPlanV2Test.CleanName.RichStringClean
import com.nec.spark.planning.SimpleSumPlanV2Test.Testing.DataSize
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.nio.file.Path
import java.nio.file.Paths

object SimpleSumPlanV2Test {

  /** Compiler-friendly name that we can use as part of class an method names. */
  final case class CleanName(value: String) {
    override def toString: String = value
  }
  object CleanName {

    implicit class RichStringClean(string: String) {
      def clean: CleanName = fromString(string)
    }
    def fromString(value: String): CleanName = CleanName(value.replaceAll("[^a-zA-Z_0-9]", ""))
  }
  abstract class Testing {
    def name: CleanName
    def verify(sparkSession: SparkSession): Unit
    def benchmark(sparkSession: SparkSession): Unit
    def prepareSession(dataSize: DataSize = DataSize.BenchmarkSize): SparkSession
    def cleanUp(sparkSession: SparkSession): Unit
    def requiresVe: Boolean
  }

  object Testing {

    /**
     * We may prepare a session with a small amount of data, but also with a big amount of data
     *
     * This enables us to confirm the *correctness* before we proceed with heavy benchmarking.
     */
    sealed trait DataSize
    object DataSize {
      case object BenchmarkSize extends DataSize
      case object SanityCheckSize extends DataSize
    }
  }

  sealed trait Source extends Serializable {
    def title: String
    def isColumnar: Boolean
    def generate(sparkSession: SparkSession): Unit
  }

  val SharedViewName = "nums"

  def makeCsvNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType)))
    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleCSV.toString)
      .withColumnRenamed("a", "value")
      .as[Double]
      .createOrReplaceTempView(SharedViewName)
  }

  lazy val PkgDir = {
    Paths.get("spark2/src/test/resources/nec/spark/planning").toAbsolutePath
  }

  lazy val SampleCSV: Path =
    PkgDir.resolve("sample.csv")

  object Source {
    case object CSV extends Source {
      override def isColumnar: Boolean = false
      override def generate(sparkSession: SparkSession): Unit = makeCsvNums(sparkSession)
      override def title: String = "CSV"
    }

    val All: List[Source] = List(CSV)
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
      source <- Source.All
      sql = "SELECT SUM(value) FROM nums"
    } yield new Testing {
      override def name: CleanName = s"SimpleSum_${source}".clean
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
          .withExtensions(_.injectColumnar())
          .withExtensions(sse =>
            sse.injectPlannerStrategy(sparkSession =>
              new Strategy {
                override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                  plan match {
                    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                      Nil
                    case _                                                                => Nil
                  }
              }
            )
          )
          .getOrCreate()

        source.generate(ss)

        ss
      }
      override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
      override def requiresVe: Boolean = false
    }
  }

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

final class SimpleSumPlanV2Test extends AnyFreeSpec with BenchTestAdditions {

  SimpleSumPlanV2Test.OurTesting.foreach(runTestCase)

}
