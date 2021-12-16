package com.nec.spark.planning

import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableInt
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeColVector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, IntegerType}

class  ProjectEvaluationPlanSpec extends AnyFlatSpec with Matchers {

  behavior of "ProjectEvaluationSpec"

  it should "correctly extract ids that should be copied if all of them are copied" in {
    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(),
      AttributeReference("NextData", IntegerType)(),
      AttributeReference("AnotherData", IntegerType)(),
      AttributeReference("YetAnotherData", IntegerType)(),
    )

    val child = StubPlan(outputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )

    assert(projectPlan.idsToCopy == List(0, 1, 2, 3))
  }

  it should "correctly extract ids if no ids are copied" in {
    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(),
      AttributeReference("NextData", IntegerType)(),
      AttributeReference("AnotherData", IntegerType)(),
      AttributeReference("YetAnotherData", IntegerType)(),
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("Data1", IntegerType)(),
      AttributeReference("Data2", IntegerType)(),
      AttributeReference("Data3", IntegerType)(),
      AttributeReference("Data4", IntegerType)(),
    )

    val child = StubPlan(childOutputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )

    assert(projectPlan.idsToCopy == List())
  }

  it should "correctly extract ids if part of the child outputs is copied" in {
    val copiedOutputs = Seq(
      AttributeReference("SomeData", IntegerType)(),
      AttributeReference("NextData", IntegerType)(),
      AttributeReference("AnotherData", IntegerType)()
    )

    val outputs: Seq[NamedExpression] = copiedOutputs ++  Seq(
      AttributeReference("NotCopiedData1", IntegerType)(),
      AttributeReference("NotCopiedData2", IntegerType)(),
      AttributeReference("NotCopiedData3", IntegerType)(),
    )

    val childOutputs: Seq[NamedExpression] = copiedOutputs ++ Seq(
      AttributeReference("SomeData1", IntegerType)(),
      AttributeReference("SomeData2", IntegerType)(),
      AttributeReference("SomeData3", IntegerType)(),
    )

    val child = StubPlan(childOutputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )

    assert(projectPlan.idsToCopy == List(0, 1, 2))
  }

  it should "correctly extract ids if non continous set of columnsIs copied" in {
    val firstCopied = AttributeReference("SomeData", IntegerType)()
    val secondCopied = AttributeReference("NextData", IntegerType)()
    val thirdCopied = AttributeReference("AnotherData", IntegerType)()

    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("NotCopiedData1", IntegerType)(),
      AttributeReference("NotCopiedData2", IntegerType)(),
      AttributeReference("NotCopiedData3", IntegerType)(),
      firstCopied,
      secondCopied,
      thirdCopied
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData1", IntegerType)(),
      firstCopied,
      AttributeReference("SomeData2", IntegerType)(),
      secondCopied,
      AttributeReference("SomeData3", IntegerType)(),
      thirdCopied
    )

    val child = StubPlan(childOutputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )

    assert(projectPlan.idsToCopy == List(1, 3 ,5))
  }

  it should "correctly build output batch if all columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
      )
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),

    )
    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(),
      AttributeReference("NextData", IntegerType)(),
      AttributeReference("AnotherData", IntegerType)(),
      AttributeReference("YetAnotherData", IntegerType)(),
    )

    val child = StubPlan(outputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )
    val outBatch = projectPlan.createOutputBatch(otherColumns, veInputBatch)

    assert(outBatch == veInputBatch)
  }

  it should "correctly build output batch if no columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
      )
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData1", IntegerType)(),
      AttributeReference("SomeData2", IntegerType)(),
      AttributeReference("SomeData3", IntegerType)(),
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "yetAnotherCol", None, VeNullableInt, 10L, List.empty),
    )

    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData", IntegerType)(),
      AttributeReference("NextData", IntegerType)(),
      AttributeReference("AnotherData", IntegerType)(),
      AttributeReference("YetAnotherData", IntegerType)(),
    )

    val child = StubPlan(childOutputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )
    val outBatch = projectPlan.createOutputBatch(otherColumns, veInputBatch)

    assert(outBatch.cols == otherColumns)
  }

  it should "correctly build output batch if some columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
      )
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "yetAnotherCol", None, VeNullableInt, 10L, List.empty),
    )
    val copiedOutputs = Seq(
      AttributeReference("SomeData", IntegerType)(),
      AttributeReference("NextData", IntegerType)(),
    )

    val outputs: Seq[NamedExpression] = copiedOutputs ++  Seq(
      AttributeReference("NotCopiedData1", IntegerType)(),
      AttributeReference("NotCopiedData2", IntegerType)(),
    )

    val childOutputs: Seq[NamedExpression] = copiedOutputs ++ Seq(
      AttributeReference("SomeData1", IntegerType)(),
      AttributeReference("SomeData2", IntegerType)(),
      AttributeReference("SomeData3", IntegerType)(),
    )

    val child = StubPlan(childOutputs)
    val projectPlan = ProjectEvaluationPlan(
      outputs.toList, null, child
    )

    val outBatch = projectPlan.createOutputBatch(otherColumns, veInputBatch)

    assert(outBatch.cols == veInputBatch.cols.take(2) ++ otherColumns.take(2))
  }

  it should "correctly create output batch ids if non continous set of columnsIs copied" in {
    val firstCopied = AttributeReference("SomeData", IntegerType)()
    val secondCopied = AttributeReference("NextData", IntegerType)()
    val thirdCopied = AttributeReference("AnotherData", IntegerType)()

    val outputs: Seq[NamedExpression] = Seq(
      AttributeReference("NotCopiedData1", IntegerType)(),
      AttributeReference("NotCopiedData2", IntegerType)(),
      AttributeReference("NotCopiedData3", IntegerType)(),
      firstCopied,
      secondCopied,
      thirdCopied
    )

    val childOutputs: Seq[NamedExpression] = Seq(
      AttributeReference("SomeData1", IntegerType)(),
      firstCopied,
      AttributeReference("SomeData2", IntegerType)(),
      secondCopied,
      AttributeReference("SomeData3", IntegerType)(),
      thirdCopied
    )

    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(0, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(0, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(0, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
        VeColVector(0, 1000, "fifthCol", None, VeNullableInt, 5L, List.empty),
        VeColVector(0, 1000, "sixthCol", None, VeNullableInt, 6L, List.empty),

      )
    )

    val otherColumns = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 11L, List.empty),
    )

    val child = StubPlan(outputs)
    val projectPlan = ProjectEvaluationPlan(
      childOutputs.toList, null, child
    )

    val outputBatch = projectPlan.createOutputBatch(otherColumns, veInputBatch)
    val expectedOutput = List(
      VeColVector(0, 1, "someCol", None, VeNullableInt, 9L, List.empty),
      VeColVector(0, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
      VeColVector(0, 2, "otherCol", None, VeNullableInt, 10L, List.empty),
      VeColVector(0, 1000, "fifthCol", None, VeNullableInt, 5L, List.empty),
      VeColVector(0, 1, "anotherCol", None, VeNullableInt, 11L, List.empty),
      VeColVector(0, 1000, "sixthCol", None, VeNullableInt, 6L, List.empty),

    )

    assert(
      outputBatch.cols == expectedOutput
    )
  }

    case class StubPlan(outputs: Seq[Expression]) extends SparkPlan {

    override lazy val outputSet: AttributeSet = AttributeSet(outputs)

    override protected def doExecute(): RDD[InternalRow] = ???

    override def output: Seq[Attribute] = ???

    override def children: Seq[SparkPlan] = ???
  }
}
