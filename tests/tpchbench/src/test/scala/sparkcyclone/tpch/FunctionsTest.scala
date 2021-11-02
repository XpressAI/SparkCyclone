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
package sparkcyclone.tpch

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.Row

class FunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  describe("isEven") {

    it("returns true if the number is even and false otherwise") {

      val data = Seq(
        (1, false),
        (2, true),
        (3, false)
      )

      val df = data
        .toDF("some_num", "expected")
        .withColumn("actual", functions.isEven(col("some_num")))

      assertColumnEquality(df, "actual", "expected")

    }

  }

}

