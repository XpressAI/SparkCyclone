package io.sparkcyclone.spark.planning

import io.sparkcyclone.spark.agile.core.VeNullableInt
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.data.vector.{VeColBatch, VeColVector}
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.spark.planning.VeProjectEvaluationPlanSpec.{SampleInputList, SampleOutputExpressions, TheSource}
import io.sparkcyclone.spark.planning.plans.VeProjectEvaluationPlan
import io.sparkcyclone.spark.planning.plans.VeProjectEvaluationPlan.ProjectionContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object VeProjectEvaluationPlanSpec {
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
final class VeProjectEvaluationPlanSpec extends AnyFlatSpec with Matchers {

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
        VeColVector(TheSource, "fourthCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 3L)
      val passFifthColVector =
        VeColVector(TheSource, "fifthCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 5L)
      val passSixthColColVector =
        VeColVector(TheSource, "sixthCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 6L)
    }
    object Compute {
      val computeSomeColVector =
        VeColVector(TheSource, "someCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 9L)
      val computeOtherColVector =
        VeColVector(TheSource, "otherCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 10L)
      val computeAnotherCol =
        VeColVector(TheSource, "anotherCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 11L)
    }

    object Ignore {
      val ignoreFirst =
        VeColVector(TheSource, "firstCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 0L)
      val ignoreSecond =
        VeColVector(TheSource, "secondCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 1L)
      val ignoreThird =
        VeColVector(TheSource, "thirdCol", VeNullableInt, numRows, Seq(1024L, 2048L), None, 2L)
    }

    import Compute._
    import Ignore._
    import PassThrough._
    val outputBatch =
      ProjectionContext(outputExpressions = SampleInputList, inputs = SampleInputList)
        .createOutputBatch(
          calculatedColumns = List(computeSomeColVector, computeOtherColVector, computeAnotherCol),
          originalBatch = VeColBatch(
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
      outputBatch.columns.toList == List(
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
    val veInputBatch = VeColBatch(
      List(
        VeColVector(TheSource, "firstCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 0L),
        VeColVector(TheSource, "secondCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 1L),
        VeColVector(TheSource, "thirdCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 2L),
        VeColVector(TheSource, "fourthCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 3L)
      )
    )

    val reusedIds = List(0, 1, 2, 3)

    val cleanedBatch = VeProjectEvaluationPlan.getBatchForPartialCleanup(reusedIds)(veInputBatch)

    assert(cleanedBatch == VeColBatch.empty)
  }

  it should "correctly cleanup batch if no columns are copied" in {
    val veInputBatch = VeColBatch(
      List(
        VeColVector(TheSource, "firstCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 0L),
        VeColVector(TheSource, "secondCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 1L),
        VeColVector(TheSource, "thirdCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 2L),
        VeColVector(TheSource, "fourthCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 3L)
      )
    )

    val cleanupBatch = VeProjectEvaluationPlan.getBatchForPartialCleanup(Seq.empty)(veInputBatch)

    assert(cleanupBatch == veInputBatch)
  }

  it should "correctly cleanup batch if some columns are copied" in {
    val copiedVectors = List(
      VeColVector(TheSource, "firstCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 0L),
      VeColVector(TheSource, "secondCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 1L),
      VeColVector(TheSource, "thirdCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 2L),
    )

    val notCopiedVectors = List(
      VeColVector(TheSource, "fourthCol", VeNullableInt, 1000, Seq(1024L, 2048L), None, 3L),
      VeColVector(TheSource, "fifth", VeNullableInt, 1000, Seq(1024L, 2048L), None, 3L),
    )

    val veInputBatch = VeColBatch(copiedVectors ++ notCopiedVectors)

    val outBatch =
      VeProjectEvaluationPlan.getBatchForPartialCleanup(List(0, 1, 2))(veInputBatch)

    assert(outBatch == VeColBatch(notCopiedVectors))
  }
}
