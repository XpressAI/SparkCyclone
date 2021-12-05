package com.nec.spark.planning

import com.nec.spark.planning.ArrowColumnarToRowPlan.mapBatchToRow
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import org.apache.arrow.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
object ArrowColumnarToRowPlan {
  def mapBatchToRow(columnarBatch: ColumnarBatch): Iterator[UnsafeRow] = {
    (0 until columnarBatch.numRows()).iterator.map { v_idx =>
      val writer = new UnsafeRowWriter(columnarBatch.numCols())
      writer.reset()
      (0 until columnarBatch.numCols()).foreach { case (c_idx) =>
        val fieldVector = columnarBatch.column(c_idx).getArrowValueVector
        if (v_idx < fieldVector.getValueCount) {
          fieldVector match {
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
