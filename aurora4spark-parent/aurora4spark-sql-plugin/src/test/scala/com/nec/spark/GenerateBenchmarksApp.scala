package com.nec.spark
import java.io.File
import java.nio.file.Files

object GenerateBenchmarksApp extends App {
  val expectedTarget = new File(args.last).getAbsoluteFile
  val methods: List[String] = {
    TestingPossibilities.possibilities.take(1).zipWithIndex.map{ case (testing, idx) =>

s"""
      @Benchmark
      def Bench${testing.name}(): Unit = {
        com.nec.spark.TestingPossibilities.possibilities(${idx}).benchmark.apply()
      }
      """
    }

  }
  Files.write(expectedTarget.toPath,
    s"""
package com.nec.spark
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class KeyBenchmark {
${methods.mkString("\n\n")}
}
  """.getBytes("UTF-8"))
}
