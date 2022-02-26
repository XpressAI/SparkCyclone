//package com.nec.cmake.eval
//
//import com.nec.arrow.{ArrowVectorBuilders, WithTestAllocator}
//import com.nec.cmake.CMakeBuilder
//import com.nec.cmake.eval.DateCastStringHoleEvaluationSpec.executeHoleEvaluation
//import com.nec.spark.agile.CExpressionEvaluation.CodeLines
//import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
//import com.nec.spark.agile.StringHole.StringHoleEvaluation
//import com.nec.spark.agile.StringHole.StringHoleEvaluation.DateCastStringHoleEvaluation
//import com.nec.spark.agile.groupby.GroupByOutline
//import com.nec.util.RichVectors.RichIntVector
//import org.scalatest.flatspec.AnyFlatSpec
//
//import java.time.LocalDate
//import java.time.format.DateTimeFormatter
//
//final class DateCastStringHoleEvaluationSpec extends AnyFlatSpec {
//  "It" should "correctly map string to date" in {
//    val list = List("1970-01-01", "2000-01-01", "1960-01-01", "2022-12-31")
//    val expectedResults = list
//      .map(LocalDate.parse(_, DateTimeFormatter.ISO_DATE))
//      .map(_.toEpochDay)
//    val evaluation = DateCastStringHoleEvaluation("strings")
//
//    val results = executeHoleEvaluation(list, evaluation)
//
//    assert(results == expectedResults)
//  }
//}
//
//object DateCastStringHoleEvaluationSpec {
//
//  def executeHoleEvaluation(
//    input: List[String],
//    stringHoleEvaluation: StringHoleEvaluation
//  ): List[Int] = {
//
//    val cLib = CMakeBuilder.buildCLogging(
//      List(
//        CFunction(
//          inputs = List(CVector.varChar("strings")),
//          outputs = List(CVector.int("dates")),
//          body = CodeLines.from(
//            stringHoleEvaluation.computeVector,
//            GroupByOutline
//              .initializeScalarVector(VeScalarType.veNullableInt, "dates", "strings->count"),
//            CodeLines.from(
//              "for ( int i = 0; i < strings->count; i++ ) { ",
//              GroupByOutline.storeTo("dates", stringHoleEvaluation.fetchResult, "i").indented,
//              "}"
//            ),
//            stringHoleEvaluation.deallocData,
//            "return 0;"
//          )
//        ).toCodeLinesG("test").cCode
//      )
//        .mkString("\n\n")
//    )
//
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
//  }
//
//}
