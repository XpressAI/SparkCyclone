package com.nec.spark.agile
import com.nec.spark.agile.BenchmarkDebugging.FileCodegen
import com.nec.spark.agile.BenchmarkDebugging.FilePlan
import com.nec.spark.agile.BenchmarkDebugging.FileResults
import com.nec.testing.Testing
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
      Paths.get(s"nec.DynamicBenchmark.${testing.name}-SingleShotTime")
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
