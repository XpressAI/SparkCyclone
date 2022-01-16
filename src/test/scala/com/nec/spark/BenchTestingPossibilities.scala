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
package com.nec.spark

import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.SampleSource.{SampleColA, SampleColB, SharedName}
import com.nec.testing.Testing.{DataSize, TestingTarget}
import com.nec.testing.{SampleSource, Testing}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.freespec.AnyFreeSpec

object BenchTestingPossibilities extends LazyLogging {

  sealed trait VeColumnMode {
    final override def toString: String = label
    def offHeapEnabled: Boolean
    def compressed: Boolean
    def label: String
  }
  object VeColumnMode {
    val All = List(OffHeapDisabled, OffHeapEnabledUnCompressed, OffHeapEnabledCompressed)
    case object OffHeapDisabled extends VeColumnMode {
      def offHeapEnabled: Boolean = false
      def compressed: Boolean = false
      def label: String = "OffHeapDisabled"
    }
    case object OffHeapEnabledUnCompressed extends VeColumnMode {
      def offHeapEnabled: Boolean = true
      def compressed: Boolean = false
      def label: String = "OffHeapEnabledUncompressed"
    }
    case object OffHeapEnabledCompressed extends VeColumnMode {
      def offHeapEnabled: Boolean = true
      def compressed: Boolean = true
      def label: String = "OffHeapEnabledCompressed"
    }
  }

  sealed trait CsvStrategy {
    def expectedString: Option[String]
    final override def toString: String = label
    def label: String
    def isNative: Boolean
  }
  object CsvStrategy {
    case object NormalCsv extends CsvStrategy {
      override def label: String = "NormalCsv"
      override def isNative: Boolean = false
      override def expectedString: Option[String] = None
    }
    val All: List[CsvStrategy] = List(
      NormalCsv
    ) //TODO: Enable the NativeCSVVe when Null handling is implemented for csv parsing
  }

  import com.eed3si9n.expecty.Expecty.assert
  final case class SimpleSql(
    sql: String,
    expectedResult: (Double, Double, Long),
    source: SampleSource,
    testingTarget: TestingTarget,
    offHeapMode: Option[VeColumnMode],
    csvStrategy: Option[CsvStrategy]
  ) extends Testing {

    type Result = (Double, Double, Long)

    override def verifyResult(result: List[Result]): Unit = {
      assert(result == List(expectedResult))
    }

    def expectedStrings: List[String] =
      testingTarget.expectedString.toList ++ csvStrategy.toList.flatMap(
        _.expectedString.toList
      ) ++ {
        if (testingTarget.isNative) List("NewCEvaluation") else Nil
      }

    override def prepareInput(sparkSession: SparkSession, dataSize: DataSize): Dataset[Result] = {
      source.generate(sparkSession, dataSize)
      import sparkSession.sqlContext.implicits._
      logger.debug(
        s"Generated input data = ${sparkSession.sql(s"SELECT * FROM ${SharedName}").collect().toList.mkString(" | ")}"
      )
      logger.info(s"Will run query: ${sql}")
      val dataSet = sparkSession.sql(sql).as[Result]

      val planString = dataSet.queryExecution.executedPlan.toString()
      logger.debug(s"Generated logical plan: ${dataSet.queryExecution.logical}")
      logger.debug(s"Generated spark plan: ${dataSet.queryExecution.sparkPlan}")
      logger.debug(s"Generated executed plan: ${planString}")
      expectedStrings.foreach { expStr =>
        assert(
          planString.contains(expStr),
          s"Expected the plan to contain '$expStr', but it didn't"
        )
      }

      dataSet
    }

    override def prepareSession(): SparkSession = {
      val sparkConf = new SparkConf(loadDefaults = true)
        .set("nec.testing.target", testingTarget.label)
        .set("nec.testing.testing", this.toString)
        .set("spark.sql.codegen.comments", "true")
      val MasterName = "local[8]"
      testingTarget match {
        case TestingTarget.Rapids =>
          SparkSession
            .builder()
            .appName(name.value)
            .master(MasterName)
            .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
            .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
            .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")
            .config(key = "spark.ui.enabled", value = false)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.VectorEngine =>
          LocalVeoExtension._enabled = true
          var builder = SparkSession
            .builder()
            .master(MasterName)
            .appName(name.value)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
            .config(key = "spark.ui.enabled", value = false)

          offHeapMode.foreach { v =>
            builder = builder
              .config(
                key = "spark.sql.columnVector.offheap.enabled",
                value = v.offHeapEnabled.toString
              )
              .config(
                key = "spark.sql.inMemoryColumnarStorage.compressed",
                value = v.compressed.toString()
              )
          }

          builder
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.PlainSpark =>
          SparkSession
            .builder()
            .master(MasterName)
            .appName(name.value)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.CMake =>
          SparkSession
            .builder()
            .master(MasterName)
            .appName(name.value)
            .withExtensions(sse => sse.injectPlannerStrategy(_ => VERewriteStrategy()))
            .config(key = "spark.com.nec.spark.batch-batches", value = "3")
            .config(CODEGEN_FALLBACK.key, value = false)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
      }
    }
  }

  val possibilities: List[Testing] =
    List(
      List(
        SimpleSql(
          sql = s"SELECT SUM(${SampleColA}), AVG(${SampleColB}), COUNT(*) FROM nums",
          expectedResult = (90.0, 4.0, 13),
          source = SampleSource.CSV,
          testingTarget = TestingTarget.Rapids,
          offHeapMode = None,
          csvStrategy = None
        )
      )
    ).flatten

  trait BenchTestAdditions extends LazyLogging { this: AnyFreeSpec =>
    def runTestCase(testing: Testing): Unit = {
      testing.name.value in {
        val sparkSession = testing.prepareSession()
        val data = testing.prepareInput(sparkSession, DataSize.SanityCheckSize)
        try {
          testing.verifyResult(data.collect().toList)
        } catch {
          case e: Throwable =>
            logger.debug(data.queryExecution.executedPlan.toString(), e)
            throw e
        } finally testing.cleanUp(sparkSession)
      }
    }
  }

  val possibilitiesMap: Map[String, Testing] =
    possibilities.map(testing => testing.name.value -> testing).toMap
}

final class BenchTestingPossibilities extends AnyFreeSpec with BenchTestAdditions {

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)

}
