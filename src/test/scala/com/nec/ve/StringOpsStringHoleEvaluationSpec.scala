//package com.nec.cmake.eval
//
//import com.eed3si9n.expecty.Expecty.expect
//import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
//import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
//import com.nec.cmake.CMakeBuilder
//import com.nec.util.RichVectors.RichIntVector
//import com.nec.spark.agile.CExpressionEvaluation.CodeLines
//import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
//import com.nec.spark.agile.StringHole
//import com.nec.spark.agile.StringHole.StringHoleEvaluation
//import com.nec.spark.agile.StringHole.StringHoleEvaluation.SlowEvaluator.SlowEvaluator
//import com.nec.spark.agile.StringHole.StringHoleEvaluation.{LikeStringHoleEvaluation, SlowEvaluator}
//import com.nec.spark.agile.groupby.GroupByOutline
//import org.scalatest.freespec.AnyFreeSpec
//
//final class StringOpsStringHoleEvaluationSpec extends AnyFreeSpec {
//
//  val list = List("this", "test", "is defi", "nitely", "tested")
//
//  "It filters strings as expected for StartsWith" in {
//    val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)
//
//    expect(
//      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
//        input = list,
//        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "test").startsWith
//      ) == testedList
//    )
//  }
//
//  "It filters strings as expected for EndsWith" in {
//    val testedList = list.map(str => if (str.endsWith("d")) 1 else 0)
//
//    expect(
//      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
//        input = list,
//        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "d").endsWith
//      ) == testedList
//    )
//  }
//
//  "It filters strings as expected for Contains" in {
//    val testedList = list.map(str => if (str.contains("s")) 1 else 0)
//
//    expect(
//      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
//        input = list,
//        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "s").contains
//      ) == testedList
//    )
//  }
//
//  "It filters strings as expected for Equals" in {
//    val testedList = list.map(str => if (str == "test") 1 else 0)
//
//    expect(
//      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
//        input = list,
//        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "test").equalsTo
//      ) == testedList
//    )
//  }
//
//  "Fast evaluator filters strings as expected for StartsWith" in {
//    val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)
//
//    expect(
//      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
//        input = list,
//        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "test").startsWith
//      ) == testedList
//    )
//  }
//
//}
//
//object StringOpsStringHoleEvaluationSpec {
//  def executeSlowEvaluator(input: List[String], slowEvaluator: SlowEvaluator): List[Int] =
//    executeHoleEvaluation(
//      input = input,
//      stringHoleEvaluation = StringHole.StringHoleEvaluation
//        .SlowEvaluation(refName = "strings", slowEvaluator = slowEvaluator)
//    )
//
//  def executeHoleEvaluation(
//    input: List[String],
//    stringHoleEvaluation: StringHoleEvaluation
//  ): List[Int] = {
//
//    val cLib = CMakeBuilder.buildCLogging(
//      List(
//        "\n\n",
//        CFunction(
//          inputs = List(CVector.varChar("strings")),
//          outputs = List(CVector.int("bools")),
//          body = CodeLines.from(
//            stringHoleEvaluation.computeVector,
//            GroupByOutline
//              .initializeScalarVector(VeScalarType.veNullableInt, "bools", "strings->count"),
//            CodeLines.from(
//              "for ( int i = 0; i < strings->count; i++ ) { ",
//              GroupByOutline.storeTo("bools", stringHoleEvaluation.fetchResult, "i").indented,
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
