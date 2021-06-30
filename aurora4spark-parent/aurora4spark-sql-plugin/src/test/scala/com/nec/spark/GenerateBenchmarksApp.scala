package com.nec.spark
import java.io.File
import java.nio.file.Files

object GenerateBenchmarksApp extends App {
  val expectedTarget = new File(args.last).getAbsoluteFile
  val fixtures: List[String] = {
    BenchTestingPossibilities.possibilities.zipWithIndex.map { case (testing, idx) =>
      s"""
@State(Scope.Benchmark)
class State_${testing.name} {
  var _sparkSession: SparkSession = null
  lazy val sparkSession: SparkSession = _sparkSession

  @Setup
  def prepare(): Unit = {
    this._sparkSession = com.nec.spark.BenchTestingPossibilities.possibilities(${idx}).prepareSession()
  }

  @TearDown
  def tearDown(): Unit = {
    com.nec.spark.BenchTestingPossibilities.possibilities(${idx}).cleanUp(sparkSession)
  }
}

      """
    }
  }

  val methods: List[String] = {
    BenchTestingPossibilities.possibilities.zipWithIndex.map { case (testing, idx) =>
      s"""
      @Benchmark
      @BenchmarkMode(Array(Mode.SingleShotTime))
      def Bench_${testing.name}(state: KeyBenchmark.State_${testing.name}): Unit = {
        com.nec.spark.BenchTestingPossibilities.possibilities(${idx}).benchmark(state.sparkSession)
      }
      """
    }

  }
  Files.write(
    expectedTarget.toPath,
    s"""
package com.nec.spark
import org.openjdk.jmh.annotations._
import org.apache.spark.sql._


object KeyBenchmark {
${fixtures.mkString("\n\n")}
}
@State(Scope.Benchmark)
class KeyBenchmark {
${methods.mkString("\n\n")}
}
  """.getBytes("UTF-8")
  )
}
