package com.nec.codegen



import scala.reflect.macros.blackbox
import scala.language.experimental.macros

object OfflineCodeGen extends App {
  val universe: scala.reflect.runtime.universe.type =
    scala.reflect.runtime.universe

  import universe._

  def generateCode() = {
    q"""package com.nec.ve.generated

       @State(Scope.Benchmark)
       class VEJMHBenchmarkCSV {

        @Benchmark
        @BenchmarkMode(Array(Mode.SingleShotTime))
        def test2JVMRunNoWholestageCodegen(sparkVeSession: SparkVeSession): Unit = {
          LocalVeoExtension._enabled = false
          val query = sparkVeSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
        }

        @Benchmark
        @BenchmarkMode(Array(Mode.SingleShotTime))
        def test2JVMRunWithWholestageCodegen(sparkWholestageSession: SparkWholestageSession): Unit = {
          val query = sparkWholestageSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
        }

        @Benchmark
        @BenchmarkMode(Array(Mode.SingleShotTime))
        def testRapidsRun(rapidsSession: SparkRapidsSession): Unit = {
          LocalVeoExtension._enabled = false
          val query = rapidsSession.sparkSession.sql("SELECT SUM(a) FROM nums_csv")
          println(query.queryExecution.executedPlan)
        }
      }

       """
}
  def saveToFile(path: String, code: Tree) = {
    val writer = new java.io.PrintWriter(path)
    try writer.write(showCode(code))
    finally writer.close()
  }

  saveToFile("/src/test/com/nec/ve/generated/JMHBe.scala", generateCode())

}
