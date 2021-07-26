package com.nec.spark
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.Float8Vector
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchToArrow extends LazyLogging {
  def fromBatch(arrowSchema: Schema, bufferAllocator: BufferAllocator)(
    columnarBatch: ColumnarBatch
  ): (VectorSchemaRoot, List[Float8Vector]) = {
    val vectors = VectorSchemaRoot.create(arrowSchema, bufferAllocator)
    val nr = columnarBatch.numRows()
    vectors.setRowCount(nr)
    val nc = columnarBatch.numCols()
    vectors -> (0 until nc).map { colId =>
      columnarBatch.column(colId) match {
        case theCol =>
          val fv = vectors.getFieldVectors.get(colId).asInstanceOf[Float8Vector]
          var rowId = 0
          while (rowId < nr) {
            fv.set(rowId, theCol.getDouble(rowId))
            rowId = rowId + 1
          }
          fv
      }
    }.toList
  }
}
