package com.nec.cmake.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowStringVector
import com.nec.arrow.CArrowNativeInterface
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.Substr
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichVarCharVector
import org.apache.arrow.vector.VarCharVector
import org.scalatest.freespec.AnyFreeSpec

object StringsCSpec {}
final class StringsCSpec extends AnyFreeSpec {
  "Substr works" in {
    val inputStrings = Seq("This is ", "Some sort", "of a test")
    withArrowStringVector(inputStrings) { vcv =>
      val outVector = new VarCharVector("value", vcv.getAllocator)
      val cLib = CMakeBuilder.buildC(
        List(TransferDefinitions.TransferDefinitionsSourceCode, Substr.SourceCode)
          .mkString("\n\n")
      )
      Substr.runOn(new CArrowNativeInterface(cLib.toString))(vcv, outVector, 1, 3)
      val expectedOutput = inputStrings.map(_.substring(1, 3)).toList

      try assert(outVector.toList == expectedOutput)
      finally outVector.close()
    }
  }
}
