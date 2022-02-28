package com.nec.cache

import com.nec.arrow.{ArrowEncodingSettings, WithTestAllocator}
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeProcessMetrics
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.freespec.AnyFreeSpec

final class SparkInternalRowsToArrowColumnarBatchesTest extends AnyFreeSpec {
  private implicit val noOpMetrics = VeProcessMetrics.noOp
  "It works" in {
    implicit val arrowEncodingSettings: ArrowEncodingSettings =
      ArrowEncodingSettings("UTC", 3, 10)
    import scala.collection.JavaConverters._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
    val result = WithTestAllocator { implicit ta =>
      SparkInternalRowsToArrowColumnarBatches
        .apply(
          Iterator(
            new GenericInternalRow(Array[Any](1, 1.0d)),
            new GenericInternalRow(Array[Any](2, 3.0d))
          ),
          new Schema(
            List(
              new Field(
                "test",
                new FieldType(false, new ArrowType.Int(8 * 4, true), null),
                List.empty.asJava
              ),
              new Field(
                "test2",
                new FieldType(
                  false,
                  new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                  null
                ),
                List.empty.asJava
              )
            ).asJava
          ),
          completeInSpark = false
        )
        .map { colBatch =>
          (0 until colBatch
            .numCols()).map(idx => colBatch.column(idx).getArrowValueVector.toString).toList
        }
        .toList
    }

    assert(result == List(List("[1, 2]", "[1.0, 3.0]")))
  }
}
