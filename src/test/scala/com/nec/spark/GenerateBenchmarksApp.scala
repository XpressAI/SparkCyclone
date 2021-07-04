package com.nec.spark
import java.io.File
import java.nio.file.Files
import com.nec.testing.Testing._

object GenerateBenchmarksApp extends App {
  val expectedTarget = new File(args.last).getAbsoluteFile
  val fixtures: List[String] = {
    BenchTestingPossibilities.possibilitiesMap.keysIterator.map { name =>
      s"""
@State(Scope.Benchmark)
class State_${name} {
  var _sparkSession: SparkSession = null
  lazy val sparkSession: SparkSession = _sparkSession

  @Setup
  def prepare(): Unit = {
    this._sparkSession = com.nec.spark.BenchTestingPossibilities.possibilitiesMap("${name}").prepareSession()
  }

  @TearDown
  def tearDown(): Unit = {
    com.nec.spark.BenchTestingPossibilities.possibilitiesMap("${name}").cleanUp(sparkSession)
  }
}

      """
    }.toList
  }

  val methods: List[String] = {
    BenchTestingPossibilities.possibilitiesMap
      /**
       * Exclude CMake as it's not really useful for benchmarking here. We are nonetheless
       * adding it to .possibilities in order to do correctness testing when in the CMake scope
       */
      .filterNot { case (name, testing) => testing.testingTarget == TestingTarget.CMake }
      .keysIterator
      .map { name =>
        s"""
      @Benchmark
      @BenchmarkMode(Array(Mode.SingleShotTime))
      def ${name}(state: DynamicBenchmark.State_${name}): Unit = {
        com.nec.spark.BenchTestingPossibilities.possibilitiesMap("${name}").benchmark(state.sparkSession)
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
