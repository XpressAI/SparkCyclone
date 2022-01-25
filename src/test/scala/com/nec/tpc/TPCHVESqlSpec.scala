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
package com.nec.tpc

import com.nec.spark.LocalVeoExtension.compilerRule
import com.nec.spark.SparkCycloneExecutorPlugin.CloseAutomatically
import com.nec.spark.planning.{VERewriteStrategy, VeColumnarRule, VeRewriteStrategyOptions}
import com.nec.spark.{AuroraSqlPlugin, SparkCycloneExecutorPlugin}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FALLBACK, WHOLESTAGE_CODEGEN_ENABLED}
import org.scalatest.ConfigMap
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Minutes, Span}

import java.io.File

object TPCHVESqlSpec {

  def VeConfiguration: SparkSession.Builder => SparkSession.Builder = {
    _.config(key = CODEGEN_FALLBACK.key, value = false)
      .config(key = "spark.sql.codegen.comments", value = true)
      .config(key = "spark.com.nec.spark.ncc.debug", value = "false")
      .config(
        key = "spark.sql.cache.serializer",
        value = "com.nec.spark.planning.VeCachedBatchSerializer"
      )
      .config(key = "spark.ui.enabled", value = true)
      .config(key = "com.nec.spark.ve.columnBatchSize", value = "500000")
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
      .withExtensions { sse =>
        sse.injectPlannerStrategy(_ => {
          new VERewriteStrategy(
            VeRewriteStrategyOptions.default.copy(failFast = true, joinOnVe = false)
          )
        })
        sse.injectColumnar(compilerRule)
        sse.injectColumnar(_ => new VeColumnarRule)
      }
  }

}

final class TPCHVESqlSpec extends TPCHSqlCSpec with TimeLimitedTests {

  override def configuration: SparkSession.Builder => SparkSession.Builder =
    TPCHVESqlSpec.VeConfiguration

  override protected def afterAll(configMap: ConfigMap): Unit = {
    SparkCycloneExecutorPlugin.closeProcAndCtx()
  }

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    // reuse the process
    CloseAutomatically = false

    val dbGenFile = new File("src/test/resources/dbgen/dbgen")
    if (!dbGenFile.exists()) {
      //s"cd ${dbGenFile.getParent} && make && ./dbgen".!
    }

    val tableFile = new File("src/test/resoruces/dbgen/lineitem.tbl")
    if (!tableFile.exists()) {
      //s"cd ${dbGenFile.getParent} && ./dbgen && popd".!
    }

    super.beforeAll(configMap)
  }

  override def timeLimit: Span = Span(35, Minutes)

}
