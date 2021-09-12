package com.nec.cmake.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.InputArrowVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.eval.RealExpressionEvaluationSpec.{
  evalFilter,
  evalGroupBySum,
  evalProject,
  evalSort
}
import com.nec.cmake.eval.StaticTypingTestAdditions._
import com.nec.spark.agile.CFunctionGeneration._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.freespec.AnyFreeSpec

import scala.runtime.LazyLong

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
      evalFilter[Double](90.0, 1.0, 2, 19, 14)(
        CExpression(cCode = "input_0->data[i] > 15", isNotNullCode = None)
      ) == List[Double](90, 19)
    )
  }

  "We can sort" in {
    expect(
      evalSort[(Double, Double)]((90.0, 5.0), (1.0, 4.0), (2.0, 2.0), (19.0, 1.0), (14.0, 3.0)) ==
        List[(Double, Double)]((19.0 -> 1.0), 2.0 -> 2.0, 14.0 -> 3.0, 1.0 -> 4.0, 90.0 -> 5.0)
    )
  }

  "We can aggregate / group by" in {
    val result = evalGroupBySum(
      List[(Double, Double, Double)]((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0))
    )(
      (
        TypedCExpression2(VeType.veNullableDouble, CExpression("input_0->data[i]", None)),
        TypedCExpression2(VeType.veNullableDouble, CExpression("input_1->data[i]", None))
      )
    )(
      (
        TypedCExpression[Double](CExpression("input_0->data[i]", None)),
        TypedCExpression[Double](CExpression("input_1->data[i]", None)),
        TypedCExpression[Double](CExpression("aggregate", None))
      )
    )
    assert(
      result ==
        List[(Double, Double, Double)]((1.0, 2.0, 7.0), (1.5, 1.2, 3.1))
    )
  }

  "We can join" in {}

}

object RealExpressionEvaluationSpec extends LazyLogging {

  def evalGroupBySum[Input, Groups, Output](
    input: List[Input]
  )(groups: (TypedCExpression2, TypedCExpression2))(expressions: Output)(implicit
    inputArguments: InputArguments[Input],
    projectExpression: ProjectExpression[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"

    val generatedSource =
      renderGroupBy(
        VeGroupBy(
          inputs = inputArguments.inputs,
          groups = List(groups._1, groups._2),
          outputs = projectExpression.outputs(expressions)
        )
      ).toCodeLines(functionName)

    logger.debug(s"Generated code: ${generatedSource.cCode}")

    val cLib = CMakeBuilder.buildCLogging(
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

  def evalFilter[Data](input: Data*)(condition: CExpression)(implicit
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

  def evalSort[Data](input: Data*)(implicit
    inputArguments: InputArguments[Data],
    outputArguments: OutputArguments[Data]
  ): List[outputArguments.Result] = {
    val functionName = "sort_f"

    val generatedSource =
      renderSort(sort =
        VeSort(
          data = inputArguments.inputs,
          sorts = List(CExpression(cCode = "input_1->data[i]", isNotNullCode = None))
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

}
