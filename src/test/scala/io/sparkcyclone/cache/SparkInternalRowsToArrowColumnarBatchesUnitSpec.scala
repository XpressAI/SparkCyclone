package io.sparkcyclone.cache

import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions._
import io.sparkcyclone.metrics.VeProcessMetrics
import scala.collection.JavaConverters._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class SparkInternalRowsToArrowColumnarBatchesUnitSpec extends AnyWordSpec {
  implicit val metrics = VeProcessMetrics.noOp
  implicit val encoding = ArrowEncodingSettings("UTC", 3, 10)
  implicit val allocator = new RootAllocator(Integer.MAX_VALUE)

  "SparkInternalRowsToArrowColumnarBatches" should {
    "work" in {
      import io.sparkcyclone.util.CallContextOps._

      val result = SparkInternalRowsToArrowColumnarBatches.apply(
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
          .numCols()).map(idx => colBatch.column(idx).extractArrowVector.get.toString).toList
      }
      .toList

      result should be (List(List("[1, 2]", "[1.0, 3.0]")))
    }
  }
}
