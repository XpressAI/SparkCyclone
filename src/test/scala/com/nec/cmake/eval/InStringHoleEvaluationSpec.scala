package com.nec.cmake.eval

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.eval.DateCastStringHoleEvaluationSpec.executeHoleEvaluation
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.StringHole.StringHoleEvaluation.{DateCastStringHoleEvaluation, InStringHoleEvaluation}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.RichVectors.RichIntVector
import org.scalatest.flatspec.AnyFlatSpec

final class InStringHoleEvaluationSpec extends AnyFlatSpec {
  "It" should  "correctly map string to date" in {
    val list = List("Dog", "Cat", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
    val toMatchList = List("Cat", "Brown", "Fox")
    val expectedResults = List()
    val evaluation = InStringHoleEvaluation("strings", toMatchList)

    val results = executeHoleEvaluation(list, evaluation)

    assert(results == expectedResults)
  }
}

object InStringHoleEvaluationSpec {

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
        ).toCodeLinesG("test").cCode
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


