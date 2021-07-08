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
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector
import org.scalatest.freespec.AnyFreeSpec

object ParseCSVSpec {
  implicit class RichFloat8(float8Vector: Float8Vector) {
    def toList: List[Double] = (0 until float8Vector.getValueCount).map(float8Vector.get).toList
  }
  implicit class RichVarCharVector(varCharVector: VarCharVector) {
    def toList: List[String] = (0 until varCharVector.getValueCount)
      .map(varCharVector.get)
      .toList
      .map(bytes => new String(bytes, "UTF-8"))
  }
  implicit class RichBigIntVector(bigIntVector: BigIntVector) {
    def toList: List[Long] = (0 until bigIntVector.getValueCount).map(bigIntVector.get).toList
  }
  implicit class RichIntVector(IntVector: IntVector) {
    def toList: List[Int] = (0 until IntVector.getValueCount).map(IntVector.get).toList
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
    val bigStr = (0 to size)
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

    val inputStr3 = "a,b\n1,2\n3,4\n"
    CsvParse.runOn2(arrowInterfaceNumeric)(Right(inputStr3), a, b)
    expect(a.toList == List[Double](1, 3), b.toList == List[Double](2, 4))

    val double0 = new Float8Vector("dbl", alloc)
    val strO = new VarCharVector("str", alloc)
    val int0 = new IntVector("ints", alloc)
    val long0 = new BigIntVector("longs", alloc)
    val inputStr4 =
      """a,b,c,d\n1.0,"one point zero",1,10000000000000\n2,twoPointZero,2,10000000000001\n"""
    CsvParse.double1str2int3long4(arrowInterfaceNumeric)(Right(inputStr4), double0, strO, int0, long0)
    expect(
      double0.toList == List[Double](1.0, 2.0),
      strO.toList == List[String]("one point zero", "twoPointZero"),
      int0.toList == List[Int](1, 2),
      long0.toList == List[Long](10000000000000L, 10000000000001L)
    )

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
