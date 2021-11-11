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
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.Float8VectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.IntVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.VarCharVectorInputWrapper
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.Float8Vector
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchToArrow extends LazyLogging {

  def fromBatch(arrowSchema: Schema, bufferAllocator: BufferAllocator)(
    columnarBatches: ColumnarBatch*
  ): (VectorSchemaRoot, List[InputVectorWrapper]) = {
    val vectors = VectorSchemaRoot.create(arrowSchema, bufferAllocator)
    val nr = columnarBatches.iterator.map(_.numRows()).sum
    vectors.setRowCount(nr)
    val colSizesSet = columnarBatches.iterator.map(_.numCols()).toSet
    require(colSizesSet.size == 1, s"Expected 1 column size only, got: ${colSizesSet}")
    val nc = columnarBatches.head.numCols()
    vectors -> (0 until nc).map { colId =>
      vectors.getVector(colId) match {
        case fv: Float8Vector =>
          var putRowId = 0
          columnarBatches.foreach { columnarBatch =>
            val theCol = columnarBatch.column(colId)
            val colRows = columnarBatch.numRows()
            var rowId = 0
            while (rowId < colRows) {
              if (theCol.isNullAt(rowId)) {
                fv.setNull(putRowId)
              } else {
                fv.set(putRowId, theCol.getDouble(rowId))
              }
              rowId = rowId + 1
              putRowId = putRowId + 1
            }
          }
          Float8VectorInputWrapper(fv)
        case fv: IntVector =>
          var putRowId = 0
          columnarBatches.foreach { columnarBatch =>
            val theCol = columnarBatch.column(colId)
            val colRows = columnarBatch.numRows()
            var rowId = 0
            while (rowId < colRows) {
              if (theCol.isNullAt(rowId)) {
                fv.setNull(putRowId)
              } else {
                fv.set(putRowId, theCol.getInt(rowId))
              }
              rowId = rowId + 1
              putRowId = putRowId + 1
            }
          }
          IntVectorInputWrapper(fv)
        case vcv: VarCharVector =>
          var putRowId = 0
          columnarBatches.foreach { columnarBatch =>
            val theCol = columnarBatch.column(colId)
            val colRows = columnarBatch.numRows()
            var rowId = 0
            while (rowId < colRows) {
              if (theCol.isNullAt(rowId)) {
                vcv.setNull(putRowId)
              } else {
                vcv.setSafe(putRowId, theCol.getUTF8String(rowId).getBytes)
              }
              rowId = rowId + 1
              putRowId = putRowId + 1
            }
          }
          VarCharVectorInputWrapper(vcv)
      }
    }.toList
  }

}
