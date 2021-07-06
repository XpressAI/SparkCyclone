package com.nec.cmake.functions

import scala.util.Random
import com.eed3si9n.expecty.Expecty.expect
import com.eed3si9n.expecty.Expecty.assert
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.CsvParse
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.verifyOn
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.sql.{Dataset, SparkSession}

object ParseCSVSpec {
  implicit class RichFloat8(float8Vector: Float8Vector) {
    def toList: List[Double] = (0 until float8Vector.getValueCount).map(float8Vector.get).toList
  }

  def verifyOn(arrowInterfaceNumeric: ArrowNativeInterfaceNumeric): Unit = {
    val inputColumns = List[(Double, Double, Double)]((1, 2, 3), (4, 5, 6), (7, 8, 9))
    val inputStr = inputColumns
      .map { case (a, b, c) => List(a, b, c).mkString(",") }
      .mkString(start = "a,b,c\n", sep = "\n", end = "\n\n")
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    val a = new Float8Vector("a", alloc)
    val b = new Float8Vector("b", alloc)
    val c = new Float8Vector("c", alloc)
    CsvParse.runOn(arrowInterfaceNumeric)(Right(inputStr), a, b, c)

    expect(
      a.toList == List[Double](1, 4, 7),
      b.toList == List[Double](2, 5, 8),
      c.toList == List[Double](3, 6, 9)
    )
    
    val inputStr2 = inputStr.replace("\n\n", "") + "\n5,43,1.2\n\n"
    CsvParse.runOn(arrowInterfaceNumeric)(Right(inputStr2), a, b, c)
    assert(a.getValueCount == 4)

    
    val size = 7
    val rng = new Random(42)
    val bigStr =  (0 to size)
      .map { case a => 
        List(
          rng.nextDouble() * rng.nextInt(1000), 
          rng.nextDouble * rng.nextInt(1000), 
          rng.nextDouble * rng.nextInt(1000)
        ).mkString(",")
      }
      .mkString(start = "a,b,c\n", sep = "\n", end = "\n\n")

    val veStart = System.currentTimeMillis()
    CsvParse.runOn(arrowInterfaceNumeric)(Right(bigStr), a, b, c)
    val veEnd = System.currentTimeMillis()
    assert(a.getValueCount == size + 1)
    /*
    val spark = SparkSession.builder().appName("CsvExample").master("local").getOrCreate()

    import spark.implicits._
    val csvData = bigStr.lines.toList.toDS()
    println("Reading csv from spark.")
    val jvmStart = System.currentTimeMillis()
    val frame = spark.read.option("header", true).option("inferSchema",true).csv(csvData)
    println(s"Spark read ${frame.count} lines")
    val jvmEnd = System.currentTimeMillis()


    println(s"Ve elapsed: ${(veEnd - veStart) / 1000}s JVM elapsed: ${(jvmEnd - jvmStart) / 1000}s")
    */
    println(s"Ve elapsed: ${(veEnd - veStart) / 1000}s")
  }
}

final class ParseCSVSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode)
        .mkString("\n\n")
    )
    verifyOn(new CArrowNativeInterfaceNumeric(cLib.toString))
  }

}
