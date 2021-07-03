package com.nec.spark.cgescape

import com.nec.spark.SampleTestData.LargeCSV
import com.nec.spark.SampleTestData.LargeParquet
import com.nec.spark.SampleTestData.SampleCSV
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import com.nec.spark.cgescape.CodegenEscapeSpec._
import com.nec.spark.SampleTestData.SampleMultiColumnCSV
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

/**
 * These tests show us how to escape Codegen.
 *
 * While they simply return the input, we are using the SUM(value) to force an aggregation, which is easy to match.
 *
 * Instead of rewriting Physical Plans we should hook into Logical plans for simplicity, as Spark's
 *
 * HashAggregateExec is split into 3 parts which are more complex to deal with.
 */
//noinspection ConvertExpressionToSAM
final class CodegenEscapeSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  private implicit val encDouble: Encoder[Double] = Encoders.scalaDouble

  "We can do a row-based batched identity codegen (accumulate results, and then process an output)" - {

    /** To achieve this, we need to first replicate how HashAggregateExec works, as that particular plan is one that loads everything into memory first, before emitting results */
    withVariousInputs[Double](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(IdentityCodegenBatchPlan(planLater(child)))
                  case _ => Nil
                }
            }
          )
        )
    )("SELECT SUM(value) FROM nums")(result => assert(result == List[Double](1, 2, 3, 4, 52)))
  }

  "We can do a row-based identity codegen" - {
    withVariousInputs[Double](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(IdentityCodegenPlan(planLater(child)))
                  case _ => Nil
                }
            }
          )
        )
    )("SELECT SUM(value) FROM nums")(result => assert(result == List[Double](1, 2, 3, 4, 52)))
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      info(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

  def withVariousInputs[T](
    configuration: SparkSession.Builder => SparkSession.Builder
  )(sql: String)(f: List[T] => Unit)(implicit enc: Encoder[T]): Unit = {
    for {
      (title, fr) <- List(
        "Memory" -> makeMemoryNums _,
        "CSV" -> makeCsvNums _,
        "Parquet" -> makeParquetNums _
      )
    } s"In ${title}" in withSparkSession2(configuration) { sparkSession =>
      import sparkSession.implicits._
      fr(sparkSession)
      val ds = sparkSession.sql(sql).debugSqlHere.as[T]
      f(ds.collect().toList)
    }
  }

}

object CodegenEscapeSpec {
  val SharedName = "nums"

  def makeMemoryNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    Seq(1d, 2d, 3d, 4d, 52d)
      .toDS()
      .createOrReplaceTempView(SharedName)
  }

  final case class SomeTab(num: Double, mapTo: Double)

  def makeCsvNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType)))

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleCSV.toString)
      .withColumnRenamed("a", "value")
      .as[Double]
      .createOrReplaceTempView(SharedName)
  }

  def makeCsvNumsMultiColumn(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType), StructField("b", DoubleType)))

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .createOrReplaceTempView(SharedName)
  }

  def makeCsvNumsLarge(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType), StructField("b", DoubleType), StructField("c", DoubleType)))

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(LargeCSV.toString)
      .withColumnRenamed("a", "value")
      .createOrReplaceTempView(SharedName)
  }

  def makeParquetNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .withColumnRenamed("a", "value")
      .as[(Double, Double)]
      .createOrReplaceTempView(SharedName)
  }

  def makeParquetNumsLarge(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.read
      .format("parquet")
      .load(LargeParquet.toString)
      .withColumnRenamed("a", "value")
      .createOrReplaceTempView(SharedName)
  }

}
