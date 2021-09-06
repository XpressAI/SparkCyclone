package com.nec.cmake.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.eval.RealExpressionEvaluationSpec.projectDoubleTf
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import com.nec.spark.agile.CFunctionGeneration._
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
object RealExpressionEvaluationSpec {

  private def projectDoubleTf: VeProjection[CVector, NamedTypedCExpression] = {
    VeProjection(
      inputs = List(CVector("input_0", VeType.veDouble)),
      outputs = List(
        NamedTypedCExpression(
          "output_0",
          VeType.VeNullableDouble,
          CExpression("2 * input_0->data[i]", isNotNullCode = None)
        ),
        NamedTypedCExpression(
          "output_1",
          VeType.VeNullableDouble,
          CExpression("2 + input_0->data[i]", isNotNullCode = None)
        )
      )
    )
  }

}

final class RealExpressionEvaluationSpec extends AnyFreeSpec {

  "We can transform a column" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val generatedSource =
      renderProjection(projectDoubleTf).toCodeLines("project_f")

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
            List(
              NativeArgument.input(vector),
              NativeArgument.output(outVector),
              NativeArgument.output(outVector2)
            )
          )

          val outFirst = outVector.toListSafe
          val outSecond = outVector2.toListSafe
          val expectedFirst: List[Option[Double]] = List[Double](180, 2, 4, 38, 28).map(Some.apply)
          val expectedSecond: List[Option[Double]] = List[Double](92, 3, 4, 21, 16).map(Some.apply)

          expect(outFirst == expectedFirst, outSecond == expectedSecond)
        } finally outVector.close()
      }
    }
  }

  "We can transform a null-column" in {

    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val generatedSource =
      renderProjection(
        VeProjection(
          inputs = List(CVector("input_0", VeType.veDouble)),
          outputs = List(
            NamedTypedCExpression(
              "output_0",
              VeType.VeNullableDouble,
              CExpression("2 * input_0->data[i]", isNotNullCode = None)
            ),
            NamedTypedCExpression(
              "output_1",
              VeType.VeNullableDouble,
              CExpression("2 + input_0->data[i]", isNotNullCode = Some("0"))
            )
          )
        )
      ).toCodeLines("project_f")

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
            List(
              NativeArgument.input(vector),
              NativeArgument.output(outVector),
              NativeArgument.output(outVector2)
            )
          )

          val outFirst = outVector.toListSafe
          val outSecond = outVector2.toListSafe
          val expectedFirst: List[Option[Double]] = List[Double](180, 2, 4, 38, 28).map(Some.apply)
          val expectedSecond: List[Option[Double]] = List[Double](92, 3, 4, 21, 16).map(_ => None)

          expect(outFirst == expectedFirst, outSecond == expectedSecond)
        } finally outVector.close()
      }
    }
  }

  "We can filter a column" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val generatedSource =
      renderFilter(filter =
        VeFilter(
          data = List(CVector("input_0", VeType.veDouble)),
          condition = CExpression(cCode = "input_0->data[i] > 15", isNotNullCode = None)
        )
      ).toCodeLines("filter_f")

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

  "We can sort" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val input2: Seq[Double] = Seq(5.0, 4.0, 2.0, 1.0, 3.0)
    val generatedSource =
      renderSort(sort =
        VeSort(
          data = List(CVector("input_0", VeType.veDouble), CVector("input_1", VeType.veDouble)),
          sorts = List(CExpression(cCode = "input_1->data[i]", isNotNullCode = None))
        )
      ).toCodeLines("sort_f")

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )
    withDirectFloat8Vector(input) { vector =>
      withDirectFloat8Vector(input2) { vector2 =>
        WithTestAllocator { alloc =>
          val outVector = new Float8Vector("value", alloc)
          val outVector2 = new Float8Vector("value2", alloc)
          try {
            val nativeInterface = new CArrowNativeInterface(cLib.toString)
            nativeInterface.callFunctionWrapped(
              "sort_f",
              List(
                NativeArgument.input(vector),
                NativeArgument.input(vector2),
                NativeArgument.output(outVector),
                NativeArgument.output(outVector2)
              )
            )
            expect(
              outVector.toList == List[Double](19, 2, 14, 1.0, 90.0),
              outVector2.toList == List[Double](1.0, 2.0, 3.0, 4.0, 5.0)
            )
          } finally {
            outVector.close()
            outVector2.close()
          }
        }
      }
    }
  }

  "We can aggregate" in {

  }

  "We can join" in {

  }

}
