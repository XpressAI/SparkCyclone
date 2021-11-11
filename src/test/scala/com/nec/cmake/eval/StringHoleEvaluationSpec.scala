package com.nec.cmake.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichIntVector
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
import com.nec.spark.agile.StringHole
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.StringHole.StringHoleEvaluation.SlowEvaluator.SlowEvaluator
import com.nec.spark.agile.StringHole.StringHoleEvaluation.{FastStartsWithEvaluation, SlowEvaluator}
import com.nec.spark.agile.groupby.GroupByOutline
import org.scalatest.freespec.AnyFreeSpec

final class StringHoleEvaluationSpec extends AnyFreeSpec {

  val list = List("this", "test", "is defi", "nitely", "tested")

  "It filters strings as expected for StartsWith" in {
    val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)

    expect(
      StringHoleEvaluationSpec.executeSlowEvaluator(
        input = list,
        slowEvaluator = SlowEvaluator.StartsWithEvaluator("test")
      ) == testedList
    )
  }

  "It filters strings as expected for EndsWith" in {
    val testedList = list.map(str => if (str.endsWith("d")) 1 else 0)

    expect(
      StringHoleEvaluationSpec.executeSlowEvaluator(
        input = list,
        slowEvaluator = SlowEvaluator.EndsWithEvaluator("d")
      ) == testedList
    )
  }

  "It filters strings as expected for Contains" in {
    val testedList = list.map(str => if (str.contains("s")) 1 else 0)

    expect(
      StringHoleEvaluationSpec.executeSlowEvaluator(
        input = list,
        slowEvaluator = SlowEvaluator.ContainsEvaluator("s")
      ) == testedList
    )
  }

  "It filters strings as expected for Equals" in {
    val testedList = list.map(str => if (str == "test") 1 else 0)

    expect(
      StringHoleEvaluationSpec.executeSlowEvaluator(
        input = list,
        slowEvaluator = SlowEvaluator.EqualsEvaluator("test")
      ) == testedList
    )
  }

  "Fast evaluator filters strings as expected for StartsWith" in {
    val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)

    expect(
      StringHoleEvaluationSpec.executeHoleEvaluation(
        input = list,
        stringHoleEvaluation = FastStartsWithEvaluation("test")
      ) == testedList
    )
  }

}

object StringHoleEvaluationSpec {
  def executeSlowEvaluator(input: List[String], slowEvaluator: SlowEvaluator): List[Int] =
    executeHoleEvaluation(
      input = input,
      stringHoleEvaluation = StringHole.StringHoleEvaluation
        .SlowEvaluation(refName = "strings", slowEvaluator = slowEvaluator)
    )

  def executeHoleEvaluation(
    input: List[String],
    stringHoleEvaluation: StringHoleEvaluation
  ): List[Int] = {

    val cLib = CMakeBuilder.buildCLogging(
      List(
        TransferDefinitionsSourceCode,
        "\n\n",
        CFunction(
          inputs = List(CVector.varChar("strings")),
          outputs = List(CVector.int("bools")),
          body = CodeLines.from(
            stringHoleEvaluation.computeVector,
            GroupByOutline
              .initializeScalarVector(VeScalarType.veNullableInt, "bools", "strings->count"),
            CodeLines.from(
              "for ( int i = 0; i < strings->count; i++ ) { ",
              GroupByOutline.storeTo("bools", stringHoleEvaluation.fetchResult, "i").indented,
              "}"
            ),
            stringHoleEvaluation.deallocData,
            "return 0;"
          )
        ).toCodeLines("test").cCode
      )
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      ArrowVectorBuilders.withArrowStringVector(input) { inVec =>
        ArrowVectorBuilders.withDirectIntVector(Seq.empty) { outVec =>
          nativeInterface.callFunction(
            name = "test",
            inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
            outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
          )
          outVec.toList
        }
      }
    }
  }

}
