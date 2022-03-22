package com.nec.ve

import com.nec.arrow.{ArrowEncodingSettings, WithTestAllocator}
import com.nec.arrow.colvector.ArrowVectorConversions._
import com.nec.cache.ColumnarBatchToVeColBatch
import com.nec.spark.SparkAdditions
import com.nec.ve.VeProcess.OriginalCallingContext
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.Ignore
import org.scalatest.freespec.AnyFreeSpec

object ColumnarBatchToVeColBatchTest {
  import collection.JavaConverters._

  val schema = new Schema(
    List(
      new Field(
        "test",
        new FieldType(false, new ArrowType.Int(8 * 4, true), null),
        List.empty.asJava
      )
    ).asJava
  )

  implicit val arrowEncodingSettings: ArrowEncodingSettings =
    ArrowEncodingSettings("UTC", 3, 10)

  val columnarBatches: List[ColumnarBatch] = {
    val col1 = new OnHeapColumnVector(5, IntegerType)
    col1.putInt(0, 1)
    col1.putInt(1, 34)
    col1.putInt(2, 9)
    col1.putInt(3, 2)
    col1.putInt(4, 3)
    new ColumnarBatch(Array(col1), 5) :: Nil
  }

}

final class ColumnarBatchToVeColBatchTest
  extends AnyFreeSpec
  with SparkAdditions
  with WithVeProcess {
  import OriginalCallingContext.Automatic._
  import ColumnarBatchToVeColBatchTest._

  "It works in row-based transformation" in {
    WithTestAllocator { implicit alloc =>
      val expectedCols: List[String] = ColumnarBatchToVeColBatch
        .toVeColBatchesViaRows(
          columnarBatches = columnarBatches.iterator,
          arrowSchema = schema,
          completeInSpark = false
        )
        .flatMap(_.cols.map(_.toBytePointerVector.toArrowVector.toString))
        .toList

      assert(expectedCols == List("[1, 34, 9]", "[2, 3]"))
    }
  }

  "It works in columnar-based transformation" in {
    WithTestAllocator { implicit alloc =>
      val gotCols: List[String] = ColumnarBatchToVeColBatch
        .toVeColBatchesViaCols(
          columnarBatches = columnarBatches.iterator,
          arrowSchema = schema,
          completeInSpark = false
        )
        .flatMap(_.cols.map(_.toBytePointerVector.toArrowVector.toString))
        .toList
      assert(gotCols == List("[1, 34, 9, 2, 3]"))
    }
  }
}
