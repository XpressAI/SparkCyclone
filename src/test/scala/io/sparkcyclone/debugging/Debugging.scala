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
package io.sparkcyclone.debugging

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.SparkPlan

object Debugging {
  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlAndShow(name: String): Dataset[T] = {
      dataSet
      // ignoring for now - a lot of noise
      /*val plansDir = Paths.get("target", "plans")
      if (!plansDir.toFile.exists()) {
        Files.createDirectory(plansDir)
      }
      val target = plansDir.resolve(s"$name.txt")
      dataSet.queryExecution.debug.toFile(target.toAbsolutePath.toString)
      Files.write(
        target,
        s"\n\n${dataSet.showAsString()}\n".getBytes("UTF-8"),
        StandardOpenOption.APPEND
      )
      dataSet*/
    }
  }

  implicit class RichSparkPlan[T](val sparkPlan: SparkPlan) {
    def debugCodegen(name: String): SparkPlan = {
      sparkPlan
      // ignoring for now - a lot of noise
      /*
      val plansDir = Paths.get("target", "plans")
      if (!plansDir.toFile.exists()) {
        Files.createDirectory(plansDir)
      }
      val target = plansDir.resolve(s"$name.txt")
      Files.write(
        target,
        org.apache.spark.sql.execution.debug.codegenString(sparkPlan).getBytes("UTF-8")
      )
      sparkPlan
      */
    }
  }

}
