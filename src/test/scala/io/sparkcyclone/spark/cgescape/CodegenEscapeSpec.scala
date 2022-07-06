/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.sparkcyclone.spark.cgescape

import io.sparkcyclone.spark.SparkAdditions
import io.sparkcyclone.testing.SampleSource.{
  makeCsvNumsMultiColumnNonNull,
  makeMemoryNums,
  makeParquetNums,
  makeParquetNumsNonNull,
  SampleColA,
  SampleColB
}
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
