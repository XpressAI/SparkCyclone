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
package io.sparkcyclone.spark.planning

import io.sparkcyclone.spark.SampleTestData
import io.sparkcyclone.spark.SparkAdditions
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

final class SparkWordCountSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  "We can do a Word count from memory and split words" in withSparkSession(identity) {
    sparkSession =>
      import sparkSession.implicits._
      List("This is", "some test", "of some stuff", "that always is").toDF
        .selectExpr("explode(split(value, ' ')) as word")
        .createOrReplaceTempView("words")

      val wordCountQuery =
        sparkSession
          .sql(
            "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC"
          )
          .as[(String, BigInt)]

      assert(wordCountQuery.collect().toList.toMap == Map("some" -> 2, "is" -> 2))
  }

  "We can do a Word count from a text file" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    sparkSession.read
      .textFile(SampleTestData.SampleStrCsv.toString)
      .selectExpr("explode(split(value, '[, ]')) as word")
      .createOrReplaceTempView("words")

    val wordCountQuery =
      sparkSession
        .sql(
          "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10"
        )
        .as[(String, BigInt)]
    assert(wordCountQuery.collect().toList.toMap == Map("some" -> 2, "is" -> 2))
  }

  "We can count-distinct some pre-split strings for a Word count" in withSparkSession(identity) {
    sparkSession =>
      import sparkSession.implicits._

      List("a", "ab", "bc", "ab")
        .toDS()
        .withColumnRenamed("value", "word")
        .createOrReplaceTempView("words")

      val wordCountQuery =
        sparkSession
          .sql(
            "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10"
          )
          .as[(String, BigInt)]

      val result = wordCountQuery.collect().toList.toMap
      assert(result == Map("ab" -> 2))

  }

  private def collectSparkPlan[U: Encoder](sparkPlan: SparkPlan): Array[U] = {
    import scala.collection.JavaConverters._
    sparkPlan.sqlContext
      .createDataFrame(
        rows = sparkPlan.executeCollectPublic().toList.asJava,
        schema = sparkPlan.schema
      )
      .as[U]
      .collect()
  }

}
