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
package com.nec.spark

import java.io.File
import java.nio.file.Files

object GenerateBenchmarksApp extends App {
  val expectedTarget = new File(args.last).getAbsoluteFile

  val fixtures: List[String] = {
    BenchTestingPossibilities.possibilitiesMap
      .filterNot(_._2.testingTarget.isCMake)
      .keysIterator
      .map { name =>
        val benchPath = s"nec.DynamicBenchmark.${name}-SingleShotTime"
        s"""
@State(Scope.Benchmark)
class State_${name} {
  lazy val testing = {
    System.setProperty("BENCH_NAME", "${name}")
    System.setProperty("BENCH_FULL_NAME", "${benchPath}")
    System.setProperty("logback.configurationFile", "./logback-bench.xml")
    com.nec.spark.BenchTestingPossibilities.possibilitiesMap("${name}")
  }
  lazy val sparkSession: SparkSession = testing.prepareSession()
  lazy val input = testing.prepareInput(sparkSession, com.nec.testing.Testing.DataSize.defaultForBenchmarks)
  lazy val benchDebugging = com.nec.spark.agile.BenchmarkDebugging(testing)
  import benchDebugging._

  @Setup
  def prepare(): Unit = {
    // initialize and also prepare the execution plan
    // I realised that the comparison includes the planning time as well
    // Depends how we want to do it, but I think we should keep it separate
    input.debugPlans()
  }

  @TearDown
  def tearDown(): Unit = {
    com.nec.spark.BenchTestingPossibilities.possibilitiesMap("${name}").cleanUp(sparkSession)
  }
}

      """
      }
      .toList
  }

  val methods: List[String] = {
    BenchTestingPossibilities.possibilitiesMap
      .filterNot(_._2.testingTarget.isCMake)
      .keysIterator
      .map { name =>
        s"""
      @Benchmark
      @BenchmarkMode(Array(Mode.SingleShotTime))
      def ${name}(state: DynamicBenchmark.State_${name}): Unit = {
        import state.benchDebugging._
        state.input.debugResults()
      }
      """
      }
      .toList
  }
  Files.write(
    expectedTarget.toPath,
    s"""
package nec
import org.openjdk.jmh.annotations._
import org.apache.spark.sql._

object DynamicBenchmark {
${fixtures.mkString("\n\n")}
}
@State(Scope.Benchmark)
class DynamicBenchmark {
${methods.mkString("\n\n")}
}
  """.getBytes("UTF-8")
  )
}
