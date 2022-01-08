package org.apache.spark.sql.vectorized

import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeType}
import com.nec.spark.planning.VeColColumnarVector
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.DualMode.RichIterator
import org.scalatest.freespec.AnyFreeSpec

final class DualModeTest extends AnyFreeSpec {
  "empty iterator gives no items" in {
    assert(Iterator.empty.distinct.isEmpty)
  }
  "one item gives one item" in {
    assert(Iterator(1).distinct.toList == List(1))
  }
  "two same items give one item" in {
    assert(Iterator(1, 1).distinct.toList == List(1))
  }
  "two same items plus another give two" in {
    assert(Iterator(1, 1, 2).distinct.toList == List(1, 2))
  }
  "two distinct items give two items" in {
    assert(Iterator(1, 2).distinct.toList == List(1, 2))
  }
  "three same items plus a different give 2" in {
    assert(Iterator(1, 1, 1, 2).distinct.toList == List(1, 2))
  }
  "three same items plus a different and one more give 3" in {
    assert(Iterator(1, 1, 1, 2, 1).distinct.toList == List(1, 2, 1))
  }

  "Accessing private class happens Ok" in {
    val vcv = VeColVector(
      VeColVectorSource("unit test"),
      3,
      "test",
      None,
      VeScalarType.veNullableInt,
      -1,
      Nil
    )
    val expectedCb = VeColBatch(numRows = vcv.numItems, cols = List(vcv))
    val cv = new VeColColumnarVector(Left(vcv), IntegerType)
    val cb = new ColumnarBatch(Array(cv))
    cb.setNumRows(2)
    import scala.collection.JavaConverters._
    val either: Either[Iterator[VeColBatch], Iterator[InternalRow]] =
      DualMode
        .handleIterator(cb.rowIterator().asScala)
        .left
        .map(_.map(lcv => VeColBatch.fromList(lcv.flatMap(_.left.toSeq))))
    assert(either.isLeft, s"Expecting left-biased result (ve col batches), got ${either}")
    val listBatches = either.left.get.toList
    assert(listBatches == List(expectedCb))
  }

}
