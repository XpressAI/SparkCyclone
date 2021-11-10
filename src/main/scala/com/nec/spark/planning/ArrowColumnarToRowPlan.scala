package com.nec.spark.planning

import com.nec.arrow.AccessibleArrowColumnVector
import com.nec.spark.planning.ArrowColumnarToRowPlan.mapBatchToRow
import org.apache.arrow.vector.{
  BigIntVector,
  BitVectorHelper,
  Float8Vector,
  IntVector,
  SmallIntVector,
  ValueVector,
  VarCharVector
}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.{
  ColumnarToRowExec,
  ColumnarToRowTransition,
  SparkPlan,
  UnaryExecNode
}
import org.apache.spark.sql.vectorized.ColumnarBatch
object ArrowColumnarToRowPlan {
  def mapBatchToRow(columnarBatch: ColumnarBatch) = {
    (0 until columnarBatch.numRows()).iterator.map { v_idx =>
      val writer = new UnsafeRowWriter(columnarBatch.numCols())
      writer.reset()
      (0 until columnarBatch.numCols()).foreach { case (c_idx) =>
        val column = columnarBatch.column(c_idx).asInstanceOf[AccessibleArrowColumnVector]
        if (v_idx < column.getArrowValueVector.getValueCount) {
          column.getArrowValueVector match {
            case vector: Float8Vector =>
              val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
              if (isNull) writer.setNullAt(c_idx)
              else writer.write(c_idx, vector.get(v_idx))
            case vector: IntVector =>
              val isNull =
                BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
              if (isNull) writer.setNullAt(c_idx)
              else writer.write(c_idx, vector.get(v_idx))
            case vector: BigIntVector =>
              val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
              if (isNull) writer.setNullAt(c_idx)
              else writer.write(c_idx, vector.get(v_idx))
            case vector: SmallIntVector =>
              val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
              if (isNull) writer.setNullAt(c_idx)
              else writer.write(c_idx, vector.get(v_idx))
            case varChar: VarCharVector =>
              val isNull = BitVectorHelper.get(varChar.getValidityBuffer, v_idx) == 0
              if (isNull) writer.setNullAt(c_idx)
              else writer.write(c_idx, varChar.get(v_idx))
          }
        }
      }
      writer.getRow
    }
  }
}

case class ArrowColumnarToRowPlan(override val child: SparkPlan) extends ColumnarToRowTransition {

  override def doExecute(): RDD[InternalRow] = {
    child
      .executeColumnar()
      .mapPartitions(batches => {
        batches.flatMap(mapBatchToRow(_))
      })
  }

  override def output: Seq[Attribute] = child.output

}
