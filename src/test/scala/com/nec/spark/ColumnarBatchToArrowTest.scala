/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.{
  Float8VectorInputWrapper,
  IntVectorInputWrapper,
  VarCharVectorInputWrapper
}
import com.nec.util.RichVectors.{RichFloat8, RichIntVector, RichVarCharVector}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.execution.arrow.ColumnarArrowWriter
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._
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
          columns.head
            .asInstanceOf[VarCharVectorInputWrapper]
            .varCharVector
            .toList() == List[String]("ABC", "DEF")
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
