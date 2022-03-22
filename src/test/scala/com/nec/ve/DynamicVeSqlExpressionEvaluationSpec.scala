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
package com.nec.ve

import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.tpc.TPCHVESqlSpec
import org.apache.spark.sql.SparkSession
import org.bytedeco.veoffload.global.veo

object DynamicVeSqlExpressionEvaluationSpec {
  def VeConfiguration: SparkSession.Builder => SparkSession.Builder =
    TPCHVESqlSpec.VeConfiguration(failFast = true)
}

final class DynamicVeSqlExpressionEvaluationSpec extends DynamicCSqlExpressionEvaluationSpec {

  override def configuration: SparkSession.Builder => SparkSession.Builder =
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration

  override protected def afterAll(): Unit = {
    SparkCycloneExecutorPlugin.closeProcAndCtx()
  }

  override protected def beforeAll(): Unit = {
    SparkCycloneExecutorPlugin._veo_proc = veo.veo_proc_create(-1)
    super.beforeAll()
  }

  "Use cyclone with spark streaming" ignore withSparkSession2(configuration) { (sparkSession: SparkSession) =>
    val table = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.parallelize(Seq(
      (1,"111"),
      (2,"111"),
      (3,"222"),
      (4,"222"),
      (5,"222"),
      (6,"111"),
      (7,"333"),
      (8,"444"),
      (9,"555")
    )))
    table.createTempView("foo")


    val df = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", 3)
      .load()


    df.createOrReplaceTempView("foo2")

    val agg = sparkSession.sql("SELECT value AS bar, COUNT(*) FROM foo2 GROUP BY value")

    println("Before writeStream")

    val queryLines = agg.writeStream
      .format("console")
      .outputMode("update")
      .start()

    println("After writeStream")

    queryLines.processAllAvailable()

    assert(true)
  }
}
