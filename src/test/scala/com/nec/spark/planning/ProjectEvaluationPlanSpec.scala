package com.nec.spark.planning

import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableInt
import com.nec.spark.planning.ProjectEvaluationPlan.ProjectionContext
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeColVector
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  AttributeSet,
  ExprId,
  NamedExpression
}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class ProjectEvaluationPlanSpec extends AnyFlatSpec with Matchers {

  behavior of "ProjectEvaluationSpec"

  it should "correctly extract ids that should be copied if all of them are copied" in {
    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(ExprId(1)),
      AttributeReference("NextData", IntegerType)(ExprId(2)),
      AttributeReference("AnotherData", IntegerType)(ExprId(3)),
      AttributeReference("YetAnotherData", IntegerType)(ExprId(4))
    )
    assert(
      ProjectionContext(
        outputExpressions = outputs,
        childOutputSet = AttributeSet(outputs)
      ).idsToPass == List(0, 1, 2, 3)
    )
  }

  it should "correctly extract ids if no ids are copied" in {
    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(ExprId(1)),
      AttributeReference("NextData", IntegerType)(ExprId(2)),
      AttributeReference("AnotherData", IntegerType)(ExprId(3)),
      AttributeReference("YetAnotherData", IntegerType)(ExprId(4))
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("Data1", IntegerType)(ExprId(5)),
      AttributeReference("Data2", IntegerType)(ExprId(6)),
      AttributeReference("Data3", IntegerType)(ExprId(7)),
      AttributeReference("Data4", IntegerType)(ExprId(8))
    )

    assert(
      ProjectionContext(
        outputExpressions = outputs,
        childOutputSet = AttributeSet(childOutputs)
      ).idsToPass == List()
    )
  }

  it should "correctly extract ids if part of the child outputs is copied" in {
    val copiedOutputs = Seq(
      AttributeReference("SomeData", IntegerType)(ExprId(1)),
      AttributeReference("NextData", IntegerType)(ExprId(2)),
      AttributeReference("AnotherData", IntegerType)(ExprId(3))
    )

    val outputs: Seq[NamedExpression] = copiedOutputs ++ Seq(
      AttributeReference("NotCopiedData1", IntegerType)(ExprId(4)),
      AttributeReference("NotCopiedData2", IntegerType)(ExprId(5)),
      AttributeReference("NotCopiedData3", IntegerType)(ExprId(6))
    )

    val childOutputs: Seq[NamedExpression] = copiedOutputs ++ Seq(
      AttributeReference("SomeData1", IntegerType)(ExprId(7)),
      AttributeReference("SomeData2", IntegerType)(ExprId(8)),
      AttributeReference("SomeData3", IntegerType)(ExprId(9))
    )

    assert(
      ProjectionContext(
        outputExpressions = outputs,
        childOutputSet = AttributeSet(childOutputs)
      ).idsToPass == List(0, 1, 2)
    )
  }

  it should "correctly extract ids if non continous set of columnsIs copied" in {
    val firstCopied = AttributeReference("SomeData", IntegerType)(ExprId(1))
    val secondCopied = AttributeReference("NextData", IntegerType)(ExprId(2))
    val thirdCopied = AttributeReference("AnotherData", IntegerType)(ExprId(3))

    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("NotCopiedData1", IntegerType)(ExprId(4)),
      AttributeReference("NotCopiedData2", IntegerType)(ExprId(5)),
      AttributeReference("NotCopiedData3", IntegerType)(ExprId(6)),
      firstCopied,
      secondCopied,
      thirdCopied
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData1", IntegerType)(ExprId(7)),
      firstCopied,
      AttributeReference("SomeData2", IntegerType)(ExprId(8)),
      secondCopied,
      AttributeReference("SomeData3", IntegerType)(ExprId(9)),
      thirdCopied
    )

    assert(
      ProjectionContext(
        outputExpressions = outputs,
        childOutputSet = AttributeSet(childOutputs)
      ).idsToPass == List(1, 3, 5)
    )
  }

  it should "correctly build output batch if all columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty)
      )
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty)
    )
    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(ExprId(1)),
      AttributeReference("NextData", IntegerType)(ExprId(2)),
      AttributeReference("AnotherData", IntegerType)(ExprId(3)),
      AttributeReference("YetAnotherData", IntegerType)(ExprId(4))
    )

    val outBatch = ProjectionContext(outputs, AttributeSet(outputs))
      .createOutputBatch(calculatedColumns = otherColumns, originalBatch = veInputBatch)

    assert(outBatch == veInputBatch)
  }

  it should "correctly build output batch if no columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty)
      )
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData1", IntegerType)(ExprId(1)),
      AttributeReference("SomeData2", IntegerType)(ExprId(2)),
      AttributeReference("SomeData3", IntegerType)(ExprId(3))
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "yetAnotherCol", None, VeNullableInt, 10L, List.empty)
    )

    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(ExprId(4)),
      AttributeReference("NextData", IntegerType)(ExprId(5)),
      AttributeReference("AnotherData", IntegerType)(ExprId(6)),
      AttributeReference("YetAnotherData", IntegerType)(ExprId(7))
    )

    val outBatch =
      ProjectionContext(outputExpressions = outputs, childOutputSet = AttributeSet(childOutputs))
        .createOutputBatch(calculatedColumns = otherColumns, originalBatch = veInputBatch)

    assert(outBatch.cols == otherColumns)
  }

  it should "correctly build output batch if some columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty)
      )
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "yetAnotherCol", None, VeNullableInt, 10L, List.empty)
    )
    val copiedOutputs = Seq(
      AttributeReference("SomeData", IntegerType)(ExprId(1)),
      AttributeReference("NextData", IntegerType)(ExprId(2))
    )

    val outputs: Seq[NamedExpression] = copiedOutputs ++ Seq(
      AttributeReference("NotCopiedData1", IntegerType)(ExprId(3)),
      AttributeReference("NotCopiedData2", IntegerType)(ExprId(4))
    )

    val childOutputs: Seq[NamedExpression] = copiedOutputs ++ Seq(
      AttributeReference("SomeData1", IntegerType)(ExprId(5)),
      AttributeReference("SomeData2", IntegerType)(ExprId(6)),
      AttributeReference("SomeData3", IntegerType)(ExprId(7))
    )

    val outBatch =
      ProjectionContext(outputExpressions = outputs, childOutputSet = AttributeSet(childOutputs))
        .createOutputBatch(calculatedColumns = otherColumns, originalBatch = veInputBatch)

    assert(outBatch.cols == veInputBatch.cols.take(2) ++ otherColumns.take(2))
  }

  it should "correctly create output batch ids if non continous set of columnsIs copied" in {
    val firstCopied = AttributeReference("SomeData", IntegerType)(ExprId(1))
    val secondCopied = AttributeReference("NextData", IntegerType)(ExprId(2))
    val thirdCopied = AttributeReference("AnotherData", IntegerType)(ExprId(3))

    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("NotCopiedData1", IntegerType)(ExprId(4)),
      AttributeReference("NotCopiedData2", IntegerType)(ExprId(5)),
      AttributeReference("NotCopiedData3", IntegerType)(ExprId(6)),
      firstCopied,
      secondCopied,
      thirdCopied
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData1", IntegerType)(ExprId(7)),
      firstCopied,
      AttributeReference("SomeData2", IntegerType)(ExprId(8)),
      secondCopied,
      AttributeReference("SomeData3", IntegerType)(ExprId(9)),
      thirdCopied
    )

    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
        VeColVector(0, 1000, "fifthCol", None, VeNullableInt, 5L, List.empty),
        VeColVector(0, 1000, "sixthCol", None, VeNullableInt, 6L, List.empty)
      )
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 11L, List.empty)
    )

    val outputBatch =
      ProjectionContext(outputExpressions = childOutputs, childOutputSet = AttributeSet(outputs))
        .createOutputBatch(calculatedColumns = otherColumns, originalBatch = veInputBatch)
    val expectedOutput = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1000, "fifthCol", None, VeNullableInt, 5L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 11L, List.empty),
      VeColVector(0, 1000, "sixthCol", None, VeNullableInt, 6L, List.empty)
    )

    assert(outputBatch.cols == expectedOutput)
  }

}
