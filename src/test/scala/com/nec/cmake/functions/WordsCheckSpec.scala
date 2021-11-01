package com.nec.cmake.functions

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichVarCharVector
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector}
import org.scalacheck.{Gen, Prop}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.Checkers

final class WordsCheckSpec extends AnyFreeSpec with Checkers {
  "it works" in {
    val someString: Gen[String] = Gen.asciiStr
    val listOfStr = Gen.listOf(someString)

    val cLib = CMakeBuilder.buildCLogging(
      List(
        TransferDefinitionsSourceCode,
        "\n\n",
        CFunction(
          inputs = List(CVector.varChar("input_0")),
          outputs = List(CVector.varChar("output_0")),
          body = CodeLines.from(
            "words_to_varchar_vector(varchar_vector_to_words(input_0), output_0);",
            "return 0;"
          )
        ).toCodeLines("test").cCode
      )
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val p: Prop = Prop.forAll(listOfStr)(list => {
        ArrowVectorBuilders.withArrowStringVector(list) { inVec =>
          ArrowVectorBuilders.withArrowStringVector(Seq.empty) { outVec =>
            nativeInterface.callFunction(
              name = "test",
              inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
              outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
            )
            outVec.toList == list
          }
        }
      })
      check(p)
    }
  }
}
