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
package com.nec.spark.agile

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class SparkSanityTests
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "It is not launched by default" in withSpark(identity) { _ =>
    assert(!SparkCycloneDriverPlugin.launched, "Expect the driver to have not been launched")
    assert(
      !SparkCycloneExecutorPlugin.launched && SparkCycloneExecutorPlugin.params.isEmpty,
      "Expect the executor plugin to have not been launched"
    )
  }

  "We can run a Spark-SQL job for a sanity test" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
    assert(result == List(3))
  }

}
