package com.nec.codegen
import scala.language.experimental.macros
import org.apache.commons.text.WordUtils
import com.nec.ve.benchmarks.{GenBenchmark, GenBenchmarkState, JvmWholestageCodegenBenchmark, VeCodegenBenchmark}
import org.reflections.Reflections
import collection.JavaConverters._
object OfflineCodeGen extends App {
  val universe: scala.reflect.runtime.universe.type =
    scala.reflect.runtime.universe

  import universe._
  def generateBenchmark(benchmark: Class[GenBenchmark[_]]): Seq[Tree] = {
    val queries = VeCodegenBenchmark.queries
    queries.map(query => {
    val typeName = TermName(
      benchmark.getSimpleName.replace("$","")
      + WordUtils.capitalizeFully(query).replaceAll("[ ()]", "")
    )
    val stateStringName = benchmark.getDeclaredField("sessionState")
      .getGenericType
      .getTypeName.replaceAll("[<>]", "")
      .split("[.]")
      .last
      val stateName = TypeName(stateStringName)
    q"""
        @Benchmark
        @BenchmarkMode(Array(Mode.SingleShotTime))
        def ${typeName.toTermName}(stateSession: ${stateName}): Unit = {
          val query = stateSession.sparkSession.sql(${query})
          println("Operation result = " + query.collect().toList)
        }
        """
  })

  }

  def generateCode() = {
    val benchmarks: Seq[Tree] = new Reflections("com.nec.ve.benchmarks")
      .getSubTypesOf(classOf[GenBenchmark[_]])
      .asScala
      .toList
      .flatMap(elem => generateBenchmark(elem.asInstanceOf[Class[GenBenchmark[_]]]))


    q"""

        package com.nec.ve.generated{
          import com.nec.ve.benchmarks._
          import com.nec.spark._
          import com.nec.ve._
          import org.openjdk.jmh.annotations._
         @State(Scope.Benchmark)
       class VEJMHBenchmarks {
          ..${benchmarks}
        }
      }

       """
}
  def saveToFile(path: String, code: Tree) = {
    val writer = new java.io.PrintWriter(path)
    try writer.write(showCode(code))
    finally writer.close()
  }

  saveToFile("/Users/wosin/aurora4spark/aurora4spark-parent/aurora4spark-sql-plugin/src/test/scala/com/nec/ve/generated/JMHBe.scala", generateCode())

}
