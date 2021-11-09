package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichIntVector
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
import com.nec.spark.agile.StringHole
import com.nec.spark.agile.StringHole.StringHoleEvaluation.SlowEvaluator
import com.nec.spark.agile.groupby.GroupByOutline
import org.scalatest.freespec.AnyFreeSpec

final class StringHoleEvaluationSpec extends AnyFreeSpec {
  "It filters strings as expected" in {
    val startsWithHole = StringHole.StringHoleEvaluation.SlowEvaluation(
      refName = "strings",
      slowEvaluator = SlowEvaluator.StartsWithEvaluator("test")
    )

    val cLib = CMakeBuilder.buildCLogging(
      List(
        TransferDefinitionsSourceCode,
        "\n\n",
        CFunction(
          inputs = List(CVector.varChar("strings")),
          outputs = List(CVector.int("bools")),
          body = CodeLines.from(
            startsWithHole.computeVector,
            GroupByOutline
              .initializeScalarVector(VeScalarType.veNullableInt, "bools", "strings->count"),
            CodeLines.from(
              "for ( int i = 0; i < strings->count; i++ ) { ",
              GroupByOutline.storeTo("bools", startsWithHole.fetchResult, "i").indented,
              "}"
            ),
            startsWithHole.deallocData,
            "return 0;"
          )
        ).toCodeLines("test").cCode
      )
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val list = List("this", "test", "is defi", "nitely", "tested")
      val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)
      ArrowVectorBuilders.withArrowStringVector(list) { inVec =>
        ArrowVectorBuilders.withDirectIntVector(Seq.empty) { outVec =>
          nativeInterface.callFunction(
            name = "test",
            inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
            outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
          )
          assert(outVec.toList == testedList)
        }
      }
    }
  }

}
