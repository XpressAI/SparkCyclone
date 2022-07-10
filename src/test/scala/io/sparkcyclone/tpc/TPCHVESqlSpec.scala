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
package io.sparkcyclone.tpc

import io.sparkcyclone.plugin.{AuroraSqlPlugin, SparkCycloneExecutorPlugin}
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.CloseAutomatically
import io.sparkcyclone.spark.transformation.{VERewriteStrategy, VeRewriteStrategyOptions}
import io.sparkcyclone.vectorengine.VeProcess
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FALLBACK, WHOLESTAGE_CODEGEN_ENABLED}
import org.scalatest.ConfigMap
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Minutes, Span}

object TPCHVESqlSpec {
  def VeConfiguration(failFast: Boolean): SparkSession.Builder => SparkSession.Builder = {
    _.config(key = CODEGEN_FALLBACK.key, value = false)
      .config(key = "spark.sql.codegen.comments", value = true)
      .config(key = "spark.cyclone.ncc.debug", value = "false")
      .config(key = "spark.ui.enabled", value = true)
      .config(key = "spark.cyclone.sql.fail-fast", value = failFast)
      .config(key = "spark.cyclone.sql.sort-on-ve", value = true)
      .config(key = "spark.cyclone.ve.columnBatchSize", value = "500000")
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
  }
}

final class TPCHVESqlSpec extends TPCHSqlCSpec with TimeLimitedTests {

  override def configuration: SparkSession.Builder => SparkSession.Builder = {
    TPCHVESqlSpec.VeConfiguration(failFast = failFast)
  }

  override def beforeAll(config: ConfigMap): Unit = {
    super.beforeAll(config)

    // Set up the process
    SparkCycloneExecutorPlugin.veProcess = VeProcess.create(-1, getClass.getName)

    // Reuse the process
    CloseAutomatically = false

    val dbGenFile = new File("src/test/resources/dbgen/dbgen")
    if (!dbGenFile.exists()) {
      //s"cd ${dbGenFile.getParent} && make && ./dbgen".!
    }

    val tableFile = new File("src/test/resoruces/dbgen/lineitem.tbl")
    if (!tableFile.exists()) {
      //s"cd ${dbGenFile.getParent} && ./dbgen && popd".!
    }
  }

  override def afterAll(config: ConfigMap): Unit = {
    SparkCycloneExecutorPlugin.veProcess.freeAll
    SparkCycloneExecutorPlugin.veProcess.close
    super.afterAll(config)
  }

  override def timeLimit: Span = {
    Span(35, Minutes)
  }
}
