package com.nec.spark
import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.freespec.AnyFreeSpec

object ColumnarBatchToArrowTest {
  lazy val schema: Schema = {
    org.apache.arrow.vector.types.pojo.Schema.fromJSON("""{
        "fields" : [ {
        "name" : "ColA",
        "nullable" : true,
        "type" : {
          "name" : "floatingpoint",
          "precision" : "DOUBLE"
        },
        "children" : [ ]
      }]
      }""")
  }
}
final class ColumnarBatchToArrowTest extends AnyFreeSpec {
  "It does not leak memory after closing" in {
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator("test columnar batch", 0L, Long.MaxValue)
    try {
      val source = new OnHeapColumnVector(2, DoubleType)
      source.putDouble(0, 1.3)
      source.putDouble(1, 1.4)
      val sampleBatch = new ColumnarBatch(Array(source))
      sampleBatch.setNumRows(2)

      val (vectorSchemaRoot, columns) =
        ColumnarBatchToArrow.fromBatch(ColumnarBatchToArrowTest.schema, allocator)(sampleBatch)
      try {
        expect(columns.size == 1, columns.head.toList == List[Double](1.3, 1.4))
      } finally vectorSchemaRoot.close()
    } finally {
      allocator.close()
    }
  }
  "Columns can be combined" in {
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator("test columnar batch", 0L, Long.MaxValue)
    try {
      val source = new OnHeapColumnVector(2, DoubleType)
      source.putDouble(0, 1.3)
      source.putDouble(1, 1.4)
      val source2 = new OnHeapColumnVector(2, DoubleType)
      source2.putDouble(0, 1.5)
      source2.putDouble(1, 1.6)
      val sampleBatch = new ColumnarBatch(Array(source), 2)
      val sampleBatch2 = new ColumnarBatch(Array(source2), 2)
      val (vectorSchemaRoot, columns) =
        ColumnarBatchToArrow.fromBatch(ColumnarBatchToArrowTest.schema, allocator)(
          sampleBatch,
          sampleBatch2
        )
      try {
        expect(columns.size == 1, columns.head.toList == List[Double](1.3, 1.4, 1.5, 1.6))
      } finally vectorSchemaRoot.close()
    } finally {
      allocator.close()
    }
  }
}
