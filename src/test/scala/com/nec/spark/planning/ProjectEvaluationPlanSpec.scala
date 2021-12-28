package com.nec.spark.planning

import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableInt
import com.nec.spark.planning.ProjectEvaluationPlanSpec.{SampleInputList, SampleOutputExpressions, TheSource}
import com.nec.spark.planning.plans.ProjectEvaluationPlan
import com.nec.spark.planning.plans.ProjectEvaluationPlan.ProjectionContext
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object ProjectEvaluationPlanSpec {
  private val SampleCol1 = AttributeReference("SomeData", IntegerType)(ExprId(1))
  private val SampleCol2 = AttributeReference("NextData", IntegerType)(ExprId(2))
  private val SampleCol3 = AttributeReference("AnotherData", IntegerType)(ExprId(3))

  private val SampleOutputExpressions: Seq[NamedExpression] = Seq(
    AttributeReference("NotCopiedData1", IntegerType)(ExprId(4)),
    AttributeReference("NotCopiedData2", IntegerType)(ExprId(5)),
    AttributeReference("NotCopiedData3", IntegerType)(ExprId(6)),
    SampleCol1,
    SampleCol2,
    SampleCol3
  )

  private val SampleInputList =
    List(
      AttributeReference("SomeData1", IntegerType)(ExprId(7)),
      SampleCol1,
      AttributeReference("SomeData2", IntegerType)(ExprId(8)),
      SampleCol2,
      AttributeReference("SomeData3", IntegerType)(ExprId(9)),
      SampleCol3
    )

  private val TheSource: VeColVectorSource = VeColVectorSource("Unit tests")

}

// ignored as currently disabled by default anwyay
@Ignore
final class ProjectEvaluationPlanSpec extends AnyFlatSpec with Matchers {

  behavior of "ProjectEvaluationSpec"

  it should "not return anything when there are no output expressions" in {
    assert(
      ProjectionContext(
        outputExpressions = Seq.empty,
        inputs = SampleInputList
      ).columnIndicesToPass.isEmpty
    )
  }

  it should "selects the right indices of columns to pass through, as only 1, 3 and 5 are referenced in the projection" in {
    assert(
      ProjectionContext(
        outputExpressions = SampleOutputExpressions,
        inputs = SampleInputList
      ).columnIndicesToPass == List(1, 3, 5)
    )
  }

  it should "correctly create output batch ids if non continous set of columnsIs copied" in {
    val numRows = 1000

    object PassThrough {
      val passFourthColVector =
        VeColVector(TheSource, numRows, "fourthCol", None, VeNullableInt, 3L, List.empty)
      val passFifthColVector =
        VeColVector(TheSource, numRows, "fifthCol", None, VeNullableInt, 5L, List.empty)
      val passSixthColColVector =
        VeColVector(TheSource, numRows, "sixthCol", None, VeNullableInt, 6L, List.empty)
    }
    object Compute {
      val computeSomeColVector =
        VeColVector(TheSource, numRows, "someCol", None, VeNullableInt, 9L, List.empty)
      val computeOtherColVector =
        VeColVector(TheSource, numRows, "otherCol", None, VeNullableInt, 10L, List.empty)
      val computeAnotherCol =
        VeColVector(TheSource, numRows, "anotherCol", None, VeNullableInt, 11L, List.empty)
    }

    object Ignore {
      val ignoreFirst =
        VeColVector(TheSource, numRows, "firstCol", None, VeNullableInt, 0L, List.empty)
      val ignoreSecond =
        VeColVector(TheSource, numRows, "secondCol", None, VeNullableInt, 1L, List.empty)
      val ignoreThird =
        VeColVector(TheSource, numRows, "thirdCol", None, VeNullableInt, 2L, List.empty)
    }

    import Compute._
    import Ignore._
    import PassThrough._
    val outputBatch =
      ProjectionContext(outputExpressions = SampleInputList, inputs = SampleInputList)
        .createOutputBatch(
          calculatedColumns = List(computeSomeColVector, computeOtherColVector, computeAnotherCol),
          originalBatch = VeColBatch.fromList(
            List(
              ignoreFirst,
              ignoreSecond,
              ignoreThird,
              passFourthColVector,
              passFifthColVector,
              passSixthColColVector
            )
          )
        )

    assert(
      outputBatch.cols == List(
        computeSomeColVector,
        passFourthColVector,
        computeOtherColVector,
        passFifthColVector,
        computeAnotherCol,
        passSixthColColVector
      )
    )
  }

  it should "correctly cleanup data if all columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(TheSource, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(TheSource, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(TheSource, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(TheSource, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty)
      )
    )

    val reusedIds = List(0, 1, 2, 3)

    val cleanedBatch = ProjectEvaluationPlan.getBatchForPartialCleanup(reusedIds)(veInputBatch)

    assert(cleanedBatch == VeColBatch(0, List.empty))
  }

  it should "correctly cleanup batch if no columns are copied" in {
    val veInputBatch = VeColBatch.fromList(
      List(
        VeColVector(TheSource, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
        VeColVector(TheSource, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
        VeColVector(TheSource, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty),
        VeColVector(TheSource, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty)
      )
    )

    val cleanupBatch = ProjectEvaluationPlan.getBatchForPartialCleanup(Seq.empty)(veInputBatch)

    assert(cleanupBatch == veInputBatch)
  }

  it should "correctly cleanup batch if some columns are copied" in {
    val copiedVectors = List(
      VeColVector(TheSource, 1000, "firstCol", None, VeNullableInt, 0L, List.empty),
      VeColVector(TheSource, 1000, "secondCol", None, VeNullableInt, 1L, List.empty),
      VeColVector(TheSource, 1000, "thirdCol", None, VeNullableInt, 2L, List.empty)
    )

    val notCopiedVectors = List(
      VeColVector(TheSource, 1000, "fourthCol", None, VeNullableInt, 3L, List.empty),
      VeColVector(TheSource, 1000, "fifth", None, VeNullableInt, 3L, List.empty)
    )

    val veInputBatch = VeColBatch.fromList(copiedVectors ++ notCopiedVectors)

    val outBatch =
      ProjectEvaluationPlan.getBatchForPartialCleanup(List(0, 1, 2))(veInputBatch)

    assert(outBatch == VeColBatch.fromList(notCopiedVectors))
  }
}
