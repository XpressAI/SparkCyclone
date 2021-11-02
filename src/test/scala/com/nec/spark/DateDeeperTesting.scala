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

import com.eed3si9n.expecty.Expecty.assert
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}

final case class DateDeeperTesting(isVe: Boolean) extends Testing {
  override type Result = Int
  private val MasterName = "local[8]"

  override def prepareSession(): SparkSession = {
    val sparkConf = new SparkConf(loadDefaults = true)
      .set("nec.testing.target", testingTarget.label)
      .set("nec.testing.testing", this.toString)
      .set("spark.sql.codegen.comments", "true")
    val builder = SparkSession
      .builder()
      .master(MasterName)
      .appName(name.value)
      .config(CODEGEN_COMMENTS.key, value = true)
      .config(key = "spark.ui.enabled", value = false)
      .config(key = "spark.sql.csv.filterPushdown.enabled", value = false)

    if (isVe)
      builder
        .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
        .config(sparkConf)
        .getOrCreate()
    else
      builder
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession => VERewriteStrategy(CNativeEvaluator))
        )
        .config(sparkConf)
        .getOrCreate()
  }

  override def prepareInput(
    sparkSession: SparkSession,
    dataSize: Testing.DataSize
  ): Dataset[Result] = {
    import sparkSession.implicits._

    val schema = StructType(
      Array(
        StructField("l_id", IntegerType),
        StructField("l_shipdate", DateType),
        StructField("l_receivedate", DateType)
      )
    )

    sparkSession.sqlContext.read
      .schema(schema)
      .option("header", "true")
      .csv(SampleTestData.SampleDateCSV2.toString)
      .createOrReplaceTempView("sample_tbl")

    // select where delivery took over 3 days
    val ds = sparkSession
      .sql("SELECT l_id FROM sample_tbl WHERE l_receivedate > date_add(l_shipdate, 3)")
      .as[Result]

//    val planString = ds.queryExecution.executedPlan.toString()
//    assert(
//      planString.contains("CEvaluation"),
//      s"Expected the plan to contain 'CEvaluation', but it didn't"
//    )
//
//    assert(
//      planString.contains("PushedFilters: []"),
//      s"Expected the plan to contain no pushed filters, but it did"
//    )
//    assert(!planString.contains("Filter ("), "Expected Filter not to be there any more")

    ds
  }

  override def verifyResult(dataset: List[Result]): Unit = {
    val expected = List[Int](1, 4)
    assert(dataset == expected)
  }

  override def testingTarget: Testing.TestingTarget =
    if (isVe) TestingTarget.VectorEngine else TestingTarget.CMake
}
