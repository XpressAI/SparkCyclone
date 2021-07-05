package com.nec.cmake.functions

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
    CsvParse.runOn(arrowInterfaceNumeric)(inputStr, a, b, c)

    expect(
      a.toList == List[Double](1, 4, 7),
      b.toList == List[Double](2, 5, 8),
      c.toList == List[Double](3, 6, 9)
    )

    CsvParse.runOn(arrowInterfaceNumeric)(inputStr + "\n5,43,1.2\n", a, b, c)

    assert(a.getValueCount == 4)
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
