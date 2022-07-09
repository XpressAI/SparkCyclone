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

import io.sparkcyclone.spark.codegen.BenchmarkDebugging.FileCodegen
import io.sparkcyclone.spark.codegen.BenchmarkDebugging.FilePlan
import io.sparkcyclone.spark.codegen.BenchmarkDebugging.FileResults
import io.sparkcyclone.testing.Testing
import org.apache.spark.sql.Dataset

import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.Path

object BenchmarkDebugging {
  val FilePlan = "plan.log"
  val FileCodegen = "codegen.log"
  val FileResults = "results.log"

  val files = List(FilePlan, FileCodegen, FileResults)
}
final case class BenchmarkDebugging(testing: Testing) {
  private lazy val dir: Path = {
    val tgtDir =
      Paths.get(s"io.sparkcyclone.benchmarks.DynamicBenchmark.${testing.name}-SingleShotTime")
    if (!Files.exists(tgtDir)) Files.createDirectories(tgtDir)
    tgtDir
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugPlans(): Unit = {
      Files.write(dir.resolve(FilePlan), dataSet.queryExecution.executedPlan.toString().getBytes())
      Files.write(
        dir.resolve(FileCodegen),
        org.apache.spark.sql.execution.debug
          .codegenString(dataSet.queryExecution.executedPlan)
          .getBytes()
      )
    }
    def debugResults(): Array[T] = {
      val array = dataSet.collect()
      Files.write(
        dir.resolve(FileResults),
        s"Results: ${array.length} rows\n\n; ${array.take(10).mkString("\n\n")}".getBytes()
      )
      val files = List(FilePlan, FileCodegen, FileResults)
      println(s"\nFiles ${files.mkString(", ")} were generated in ${dir.toAbsolutePath}")
      array
    }
  }

}
