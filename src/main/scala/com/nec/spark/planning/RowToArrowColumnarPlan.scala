package com.nec.spark.planning

import scala.collection.JavaConverters.asScalaBufferConverter

import com.nec.arrow.AccessibleArrowColumnVector
import com.nec.spark.planning.RowToArrowColumnarPlan.collectInputRows
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
object RowToArrowColumnarPlan {
  def collectInputRows(
    rows: Iterator[InternalRow],
    arrowSchema: org.apache.arrow.vector.types.pojo.Schema
  )(implicit allocator: BufferAllocator): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    rows.foreach { row =>
      arrowWriter.write(row)
    }
    arrowWriter.finish()
    root
  }
}
class RowToArrowColumnarPlan(override val child: SparkPlan) extends RowToColumnarExec(child) {

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
      .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)
    val timeZoneId = conf.sessionLocalTimeZone

    val rows = child
      .execute()
      .mapPartitions(rowIt => {
        Iterator {
          val root =
            collectInputRows(rowIt, ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId))
          val size = new ColumnarBatch(
            root.getFieldVectors.asScala
              .map(vector => new AccessibleArrowColumnVector(vector))
              .toArray,
            root.getRowCount
          )
          size
        }
      })

    rows
  }

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def output: Seq[Attribute] = child.output

}
