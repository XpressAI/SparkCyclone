package com.nec.cmake.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import com.nec.spark.agile.ExprEvaluation2
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ExpressionEvaluationSpec extends AnyFreeSpec {
  "We can transform a column" in {

    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val generatedSource = ExprEvaluation2.projectDouble

    println(generatedSource.cCode)
    System.out.flush()
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    withDirectFloat8Vector(input) { vector =>
      WithTestAllocator { alloc =>
        val outVector = new Float8Vector("value", alloc)
        val outVector2 = new Float8Vector("value2", alloc)
        try {
          val nativeInterface = new CArrowNativeInterface(cLib.toString)
          nativeInterface.callFunctionWrapped(
            "project_f",
            List(NativeArgument.input(vector), NativeArgument.output(outVector), NativeArgument.output(outVector2))
          )

          val outFirst = outVector.toListSafe
          val outSecond = outVector2.toListSafe
          val expectedFirst: List[Option[Double]] = List[Double](180, 2, 4, 38, 28).map(Some.apply)
          val expectedSecond: List[Option[Double]] = List[Double](92, 3, 4, 21, 16).map(Some.apply)

          expect(
            outFirst == expectedFirst,
            outSecond == expectedSecond,
          )
        } finally outVector.close()
      }
    }
  }

  "We can filter a column" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val generatedSource = ExprEvaluation2.filterDouble

    info(generatedSource.cCode)
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )
    withDirectFloat8Vector(input) { vector =>
      WithTestAllocator { alloc =>
        val outVector = new Float8Vector("value", alloc)
        try {
          val nativeInterface = new CArrowNativeInterface(cLib.toString)
          nativeInterface.callFunctionWrapped(
            "filter_f",
            List(NativeArgument.input(vector), NativeArgument.output(outVector))
          )
          val outData = (0 until outVector.getValueCount).map(idx => outVector.get(idx)).toList
          assert(outData == List[Double](90, 19))
        } finally outVector.close()
      }
    }
  }
  "We can aggregate" in {

  }
  "We can join" in {

  }
}
