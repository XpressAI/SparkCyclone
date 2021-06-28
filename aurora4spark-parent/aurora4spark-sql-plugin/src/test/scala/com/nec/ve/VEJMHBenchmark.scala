package com.nec.ve

import java.io.FileWriter

import com.nec.spark._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.runner.options.Options

object VEJMHBenchmark {
  val template =
    """
      |package com.nec.ve
      |
      |import com.nec.spark._
      |import org.openjdk.jmh.annotations._
      |@State(Scope.Benchmark)
      |class VEJMHBenchmarkCSVGen {
      |
      |  @Benchmark
      |  @BenchmarkMode(Array(Mode.SingleShotTime))
      |  def test2JVMRunNoWholestageCodegen(sparkVeSession: SparkVeSession): Unit = {
      |    LocalVeoExtension._enabled = false
      |    val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
      |    println(s"JVM result = ${query.collect().toList}")
      |  }
      |
      |  @Benchmark
      |  @BenchmarkMode(Array(Mode.SingleShotTime))
      |  def test2JVMRunWithWholestageCodegen(sparkWholestageSession: SparkWholestageSession): Unit = {
      |    val query = sparkWholestageSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
      |    println(s"JVM result = ${query.collect().toList}")
      |  }
      |
      |  @Benchmark
      |  @BenchmarkMode(Array(Mode.SingleShotTime))
      |  def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
      |    LocalVeoExtension._enabled = false
      |    val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
      |    println(query.queryExecution.executedPlan)
      |    println(s"JVM result = ${query.collect().toList}")
      |  }
      |}
      |""".stripMargin


  def main(args: Array[String]): Unit = {
    val fileWriter = new FileWriter("./src/test/scala/com/nec/ve/VEJMHBenchmarkGen.scala")
    fileWriter.write(template)
    fileWriter.flush()
  }
}
