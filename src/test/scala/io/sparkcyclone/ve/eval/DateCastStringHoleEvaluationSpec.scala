package io.sparkcyclone.ve.eval

import io.sparkcyclone.native.cmake.CMakeBuilder
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.spark.agile.core._
import io.sparkcyclone.spark.agile.CFunctionGeneration.CFunction
import io.sparkcyclone.spark.agile.StringHole.StringHoleEvaluation
import io.sparkcyclone.spark.agile.StringHole.StringHoleEvaluation.DateCastStringHoleEvaluation
import io.sparkcyclone.spark.agile.groupby.GroupByOutline
import io.sparkcyclone.ve.eval.DateCastStringHoleEvaluationSpec.executeHoleEvaluation
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter

@Ignore
@VectorEngineTest
final class DateCastStringHoleEvaluationSpec extends AnyFlatSpec {
  "It" should "correctly map string to date" in {
    val list = List("1970-01-01", "2000-01-01", "1960-01-01", "2022-12-31")
    val expectedResults = list
      .map(LocalDate.parse(_, DateTimeFormatter.ISO_DATE))
      .map(_.toEpochDay)
    val evaluation = DateCastStringHoleEvaluation("strings")

    val results = executeHoleEvaluation(list, evaluation)

    assert(results == expectedResults)
  }
}

object DateCastStringHoleEvaluationSpec {

  def executeHoleEvaluation(
    input: List[String],
    stringHoleEvaluation: StringHoleEvaluation
  ): List[Int] = {

    val cLib = CMakeBuilder.buildCLogging(
      List(
        CFunction(
          inputs = List(CVector.varChar("strings")),
          outputs = List(CVector.int("dates")),
          body = CodeLines.from(
            stringHoleEvaluation.computeVector,
            GroupByOutline
              .initializeScalarVector(VeNullableInt, "dates", "strings->count"),
            CodeLines.from(
              "for ( int i = 0; i < strings->count; i++ ) { ",
              GroupByOutline.storeTo("dates", stringHoleEvaluation.fetchResult, "i").indented,
              "}"
            ),
            "return 0;"
          )
        ).toCodeLinesS("test").cCode
      )
        .mkString("\n\n")
    )

    Nil

//    val nativeInterface = new CArrowNativeInterface(cLib.toString)
//    WithTestAllocator { implicit allocator =>
//      ArrowVectorBuilders.withArrowStringVector(input) { inVec =>
//        ArrowVectorBuilders.withDirectIntVector(Seq.empty) { outVec =>
//          nativeInterface.callFunction(
//            name = "test",
//            inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
//            outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
//          )
//          outVec.toList
//        }
//      }
//    }
  }

}
