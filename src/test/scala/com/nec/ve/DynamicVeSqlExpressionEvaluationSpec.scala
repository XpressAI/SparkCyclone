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
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.tpc.TPCHVESqlSpec
import org.apache.spark.sql.SparkSession
import org.bytedeco.veoffload.global.veo

import java.util.{Timer, TimerTask}

object DynamicVeSqlExpressionEvaluationSpec {
  def VeConfiguration: SparkSession.Builder => SparkSession.Builder =
    TPCHVESqlSpec.VeConfiguration(failFast = true)
}

@VectorEngineTest
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



  "Use cyclone with spark streaming" in withSparkSession2(configuration) { (sparkSession: SparkSession) =>
    import sparkSession.implicits._

    val table = Seq(
      Foo(1,"111"),
      Foo(2,"111"),
      Foo(3,"222"),
      Foo(4,"222"),
      Foo(5,"222"),
      Foo(6,"111"),
      Foo(7,"333"),
      Foo(8,"444"),
      Foo(9,"555")
    ).toDS()
    table.createTempView("foo")

    val res = sparkSession.sql("SELECT id, COUNT(*) FROM foo GROUP BY id")
    res.explain(true)
    res.collect().foreach(println)


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

    val currentThread = Thread.currentThread()
    new Thread(() => {
      val timer = new Timer()
      timer.schedule(new TimerTask {
        override def run(): Unit = {
          currentThread.interrupt()
        }
      }, 10 * 1000)
    }).start()

    try {
      queryLines.awaitTermination()
    } catch {
      case e: InterruptedException =>
        // pass
    }

    assert(true)
  }
}

case class Foo(id: Int, a: String)