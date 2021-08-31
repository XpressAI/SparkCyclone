package com.nec.spark

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.DateDayVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.Float8VectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.IntVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.VarCharVectorInputWrapper
import com.nec.cmake.functions.ParseCSVSpec.{
  RichDateVector,
  RichFloat8,
  RichIntVector,
  RichVarCharVector
}
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.execution.arrow.ColumnarArrowWriter
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.freespec.AnyFreeSpec

import java.time.LocalDate

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

  "Strings can be transferred" in {
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator("test columnar batch", 0L, Long.MaxValue)
    try {
      val source = new OnHeapColumnVector(2, StringType)
      source.putByteArray(0, "ABC".getBytes())
      source.putByteArray(1, "DEF".getBytes())
      val sampleBatch = new ColumnarBatch(Array(source), 2)

      val sparkSchema = StructType(Array(StructField("v", StringType)))
      val arrowSchema = ColumnarArrowWriter.arrowSchemaFor(sparkSchema, "UTC")

      val (vectorSchemaRoot, columns) =
        ColumnarBatchToArrow.fromBatch(arrowSchema, allocator)(sampleBatch)
      try {
        expect(
          columns.size == 1,
          columns.head.asInstanceOf[VarCharVectorInputWrapper].varCharVector.toList == List[String](
            "ABC",
            "DEF"
          )
        )
      } finally vectorSchemaRoot.close()
    } finally {
      allocator.close()
    }
  }

  "Ints can be transferred" in {
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator("test columnar batch", 0L, Long.MaxValue)
    try {
      val source = new OnHeapColumnVector(2, IntegerType)
      source.putInt(0, 1234)
      source.putInt(1, 12457)
      val sampleBatch = new ColumnarBatch(Array(source), 2)

      val sparkSchema = StructType(Array(StructField("v", IntegerType)))
      val arrowSchema = ColumnarArrowWriter.arrowSchemaFor(sparkSchema, "UTC")

      val (vectorSchemaRoot, columns) =
        ColumnarBatchToArrow.fromBatch(arrowSchema, allocator)(sampleBatch)
      try {
        expect(
          columns.size == 1,
          columns.head.asInstanceOf[IntVectorInputWrapper].intVector.toList == List[Int](
            1234,
            12457
          )
        )
      } finally vectorSchemaRoot.close()
    } finally {
      allocator.close()
    }
  }

  "It does not leak memory after closing" in {
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator("test columnar batch", 0L, Long.MaxValue)
    try {
      val source = new OnHeapColumnVector(2, DoubleType)
      source.putDouble(0, 1.3)
      source.putDouble(1, 1.4)
      val sampleBatch = new ColumnarBatch(Array(source), 2)
      val (vectorSchemaRoot, columns) =
        ColumnarBatchToArrow.fromBatch(ColumnarBatchToArrowTest.schema, allocator)(sampleBatch)
      try {
        expect(
          columns.size == 1,
          columns.head.asInstanceOf[Float8VectorInputWrapper].float8Vector.toList == List[Double](
            1.3,
            1.4
          )
        )
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
        expect(
          columns.size == 1,
          columns.head.asInstanceOf[Float8VectorInputWrapper].float8Vector.toList == List[Double](
            1.3,
            1.4,
            1.5,
            1.6
          )
        )
      } finally vectorSchemaRoot.close()
    } finally {
      allocator.close()
    }
  }
}
