package com.nec.spark
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.Float8VectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.IntVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.VarCharVectorInputWrapper
import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.Float8Vector
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ArrowColumnVector
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
            theCol match {
              case HasFloat8Vector(f8v) =>
                // transfer without copying - the ownership goes to the new vector
                f8v.transferTo(fv)
              case _ =>
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
