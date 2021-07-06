package com.nec.spark
import com.nec.testing.GenericTesting

import java.io.File
import java.nio.file.Files

object GenerateBenchmarksApp extends App {
  val expectedTarget = new File(args.last).getAbsoluteFile
  val fixtures: List[String] = {
    BenchTestingPossibilities.possibilitiesMap.keysIterator.map { name =>
      s"""
@State(Scope.Benchmark)
class State_${name} {
  lazy val testing = com.nec.spark.BenchTestingPossibilities.possibilitiesMap("${name}")
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
    }.toList
  } ++ {
    GenericTesting.possibilitiesMap.keysIterator.map { name =>
      s"""
@State(Scope.Benchmark)
class State_${name} {
  val testing = com.nec.testing.GenericTesting.possibilitiesMap("${name}")
  var state: testing.State = _

  def executeTest(): Unit = {
    testing.benchmark(state)
  }

  @Setup
  def prepare(): Unit = {
    this.state = testing.init()
  }

  @TearDown
  def tearDown(): Unit = {
    testing.cleanUp(state)
  }
}
      """
    }
  }

  val methods: List[String] = {
    BenchTestingPossibilities.possibilitiesMap
      /**
       * Exclude CMake as it's not really useful for benchmarking here. We are nonetheless
       * adding it to .possibilities in order to do correctness testing when in the CMake scope
       *
       * Bringing it back -- useful for local development just to get an idea
       * As no VE here
       */
//      .filterNot { case (name, testing) => testing.testingTarget == TestingTarget.CMake }
      .keysIterator.map { name =>
        s"""
      @Benchmark
      @BenchmarkMode(Array(Mode.SingleShotTime))
      def ${name}(state: DynamicBenchmark.State_${name}): Unit = {
        import state.benchDebugging._
        state.input.debugResults()
      }
      """
      }.toList ++ GenericTesting.possibilitiesMap.keysIterator.map { name =>
      s"""
      @Benchmark
      @BenchmarkMode(Array(Mode.SingleShotTime))
      def ${name}(state: DynamicBenchmark.State_${name}): Unit = {
        state.executeTest()
      }
      """
    }
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
