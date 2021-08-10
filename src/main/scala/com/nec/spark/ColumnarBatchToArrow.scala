package com.nec.spark
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.Float8Vector
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchToArrow extends LazyLogging {
  def fromBatch(arrowSchema: Schema, bufferAllocator: BufferAllocator)(
    columnarBatches: ColumnarBatch*
  ): (VectorSchemaRoot, List[Float8Vector]) = {
    val vectors = VectorSchemaRoot.create(arrowSchema, bufferAllocator)
    val nr = columnarBatches.iterator.map(_.numRows()).sum
    vectors.setRowCount(nr)
    vectors.getFieldVectors.asScala.foreach(fieldVector => fieldVector.setValueCount(nr))
    val colSizesSet = columnarBatches.iterator.map(_.numCols()).toSet
    require(colSizesSet.size == 1, s"Expected 1 column size only, got: ${colSizesSet}")
    val nc = columnarBatches.head.numCols()
    vectors -> (0 until nc).map { colId =>
      val fv = vectors.getFieldVectors.get(colId).asInstanceOf[Float8Vector]
      var putRowId = 0
      columnarBatches.foreach { columnarBatch =>
        val theCol = columnarBatch.column(colId)
        val colRows = columnarBatch.numRows()
        var rowId = 0
        while (rowId < colRows) {
          fv.set(putRowId, theCol.getDouble(rowId))
          rowId = rowId + 1
          putRowId = putRowId + 1
        }
      }
      fv
    }.toList
  }
}
