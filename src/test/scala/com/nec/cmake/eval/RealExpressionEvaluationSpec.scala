package com.nec.cmake.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.InputArrowVectorWrapper
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.eval.RealExpressionEvaluationSpec.{evalFilter, evalProject}
import com.nec.cmake.eval.StaticTypingTestAdditions.TypedCExpression
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import com.nec.spark.agile.CFunctionGeneration._
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec
import StaticTypingTestAdditions._

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
final class RealExpressionEvaluationSpec extends AnyFreeSpec {

  "We can transform a column" in {
    expect(
      evalProject(List[Double](90.0, 1.0, 2, 19, 14))(
        TypedCExpression[Double](CExpression("2 * input_0->data[i]", None)),
        TypedCExpression[Double](CExpression("2 + input_0->data[i]", None))
      ) == List[(Double, Double)]((180, 92), (2, 3), (4, 4), (38, 21), (28, 16))
    )
  }

  "We can transform a null-column" in {
    expect(
      evalProject(List[Double](90.0, 1.0, 2, 19, 14))(
        TypedCExpression[Double](CExpression("2 * input_0->data[i]", None)),
        TypedCExpression[Option[Double]](CExpression("2 + input_0->data[i]", Some("0")))
      ) == List[(Double, Option[Double])]((180, None), (2, None), (4, None), (38, None), (28, None))
    )
  }

  "We can filter a column" in {
    expect(
      evalFilter(List[Double](90.0, 1.0, 2, 19, 14))(
        CExpression(cCode = "input_0->data[i] > 15", isNotNullCode = None)
      ) == List[Double](90, 19)
    )
  }

  "We can sort" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val input2: Seq[Double] = Seq(5.0, 4.0, 2.0, 1.0, 3.0)
    val generatedSource =
      renderSort(sort =
        VeSort(
          data = List(
            CVector("input_0", VeType.veNullableDouble),
            CVector("input_1", VeType.veNullableDouble)
          ),
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

  "We can aggregate" in {}

  "We can join" in {}

}

object RealExpressionEvaluationSpec {

  def evalProject[Input, Output](input: List[Input])(expressions: Output)(implicit
    inputArguments: InputArguments[Input],
    projectExpression: ProjectExpression[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"

    val generatedSource =
      renderProjection(
        VeProjection(
          inputs = inputArguments.inputs,
          outputs = projectExpression.outputs(expressions)
        )
      ).toCodeLines(functionName)

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalFilter[Data](input: List[Data])(condition: CExpression)(implicit
    inputArguments: InputArguments[Data],
    outputArguments: OutputArguments[Data]
  ): List[outputArguments.Result] = {
    val functionName = "filter_f"

    val generatedSource =
      renderFilter(VeFilter(data = inputArguments.inputs, condition = condition))
        .toCodeLines(functionName)

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

}
