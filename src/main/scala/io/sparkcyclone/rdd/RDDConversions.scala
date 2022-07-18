package io.sparkcyclone.rdd

import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.transfer.{BpcvTransferDescriptor, RowCollectingTransferDescriptor}
import io.sparkcyclone.data.vector.{ByteArrayColBatch, VeColBatch}
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.util.CallContextOps._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.catalyst.InternalRow

object RDDConversions {
  implicit class InternalRowRDDConversions(rdd: RDD[InternalRow]) {
    def toVeColBatchRDD(attributes: Seq[Attribute],
                        targetBatchSize: Int): RDD[VeColBatch] = {
      rdd.mapPartitions { rows =>
        new Iterator[VeColBatch] {
          override def hasNext: Boolean = {
            rows.hasNext
          }

          override def next: VeColBatch = {
            var currentRowCount = 0
            val descriptor = RowCollectingTransferDescriptor(attributes, targetBatchSize)

            while (rows.hasNext && currentRowCount < targetBatchSize) {
              descriptor.append(rows.next)
              currentRowCount += 1
            }

            vectorEngine.executeTransfer(descriptor)
          }
        }
      }
    }
  }

  implicit class ColumnarBatchRDDConversions(rdd: RDD[ColumnarBatch]) {
    def toVeColBatchRDD(schema: Schema): RDD[VeColBatch] = {
      rdd.mapPartitions { colbatches =>
        val descriptor = colbatches
          .foldLeft(new BpcvTransferDescriptor.Builder()) { case (builder, colbatch) =>
            builder.newBatch().addColumns(colbatch.toBytePointerColBatch(schema).columns)
          }
          .build()

        if (descriptor.isEmpty) {
          Iterator.empty
        } else {
          Seq(vectorEngine.executeTransfer(descriptor)).iterator
        }
      }
    }
  }
}
