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
package io.sparkcyclone.spark.codegen

import java.util.Properties
import io.sparkcyclone.h2.H2DatabaseConnector
import io.sparkcyclone.spark.SampleTestData
import io.sparkcyclone.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class RealWorldSparkPlansSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {
  before {
    H2DatabaseConnector.init()
  }
  "We get the spark plan for more complex query" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val props = new Properties()

    sparkSession.read
      .jdbc(H2DatabaseConnector.inputH2Url, "Users", props)
      .createOrReplaceTempView("Users")

    sparkSession.read
      .option("header", "true")
      .csv(SampleTestData.OrdersCsv.toString)
      .createOrReplaceTempView("Orders")

    sparkSession
      .sql(sqlText =
        "SELECT SUM(totalPrice), userId, joined_timestamp FROM Users JOIN Orders on Orders.userId = Users.id " +
          "GROUP BY userId, joined_timestamp ORDER BY joined_timestamp ASC"
      )
  }
}
