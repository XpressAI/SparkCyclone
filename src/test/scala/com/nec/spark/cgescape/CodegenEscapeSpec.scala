package com.nec.spark.cgescape

import com.nec.spark.SparkAdditions
import com.nec.testing.SampleSource.{SampleColA, SampleColB, makeCsvNumsMultiColumnNonNull, makeMemoryNums, makeParquetNums, makeParquetNumsNonNull}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK

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
  private implicit val encDouble2: Encoder[(Double, Double)] = Encoders.tuple(encDouble, encDouble)

  "We can do a row-based batched identity codegen (accumulate results, and then process an output)" - {
    /** To achieve this, we need to first replicate how HashAggregateExec works, as that particular plan is one that loads everything into memory first, before emitting results */
    withVariousInputs[(Double, Double)](
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
    )(s"SELECT SUM(${SampleColA}), SUM(${SampleColB}) FROM nums")(result =>
      assert(result.map(_._1).sorted == List[Double](1, 2, 3, 4, 52).sorted)
    )
  }

  "We can do a row-based identity codegen" - {
    withVariousInputs[(Double, Double)](
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
    )(s"SELECT SUM(${SampleColA}), SUM(${SampleColB}) FROM nums")(result =>
      assert(result.map(_._1).sorted == List[Double](1, 2, 3, 4, 52).sorted)
    )
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere[V](f: Dataset[T] => V): V = {
      withClue(dataSet.queryExecution.executedPlan.toString()) {
        f(dataSet)
      }
    }
  }

  def withVariousInputs[T](
    configuration: SparkSession.Builder => SparkSession.Builder
  )(sql: String)(f: List[T] => Unit)(implicit enc: Encoder[T]): Unit = {
    for {
      (title, fr) <- List(
        "Memory" -> makeMemoryNums _,
        "CSV" -> makeCsvNumsMultiColumnNonNull _,
        "Parquet" -> makeParquetNumsNonNull _
      )
    } s"In ${title}" in withSparkSession2(configuration) { sparkSession =>

      fr(sparkSession)
      sparkSession.sql(sql).debugSqlHere { ds =>
        f(ds.as[T].collect().toList)
      }
    }
  }

}
