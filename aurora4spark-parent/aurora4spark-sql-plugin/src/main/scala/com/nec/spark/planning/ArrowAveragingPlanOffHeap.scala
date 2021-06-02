package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Sum
import com.nec.spark.agile.{ColumnAggregation, OutputColumnAggregated}
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import com.nec.spark.agile.Column
import org.apache.arrow.memory.RootAllocator

object ArrowAveragingPlanOffHeap {

  trait MultipleColumnsOffHeapAverager extends Serializable {
    def avg(interface: ArrowNativeInterfaceNumeric, float8Vector: Float8Vector,
            columns: Int): Double
  }

  object MultipleColumnsOffHeapAverager extends MultipleColumnsOffHeapAverager {
    override def avg(interface: ArrowNativeInterfaceNumeric, float8Vector: Float8Vector,
                     columns: Int): Double = {
      Sum
        .runOn(interface)(float8Vector, columns)
        .head
    }
  }
}

case class ArrowAveragingPlanOffHeap(
  child: SparkPlan,
  nativeInterface: ArrowNativeInterfaceNumeric,
  column: Column
) extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .mapPartitions { columnar =>
        val ra = new RootAllocator()
        val vector = new Float8Vector("value", ra)
        val allNumbers = columnar
          .flatMap(columnar => columnar.column(column.index).getDoubles(0, columnar.numRows()))
          .toList
        vector.allocateNew(allNumbers.size)
        allNumbers.zipWithIndex
          .foreach{
            case (elem, idx) => vector.setSafe(idx, elem)
          }
        vector.setValueCount(allNumbers.size)
        val result = (Sum.runOn(nativeInterface)(vector, 1).head, vector.getValueCount)
        Iterator(result)
      }
      .coalesce(1)
      .mapPartitions{  it =>
        val result = it.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        val outVector = new OffHeapColumnVector(1, DoubleType)
        val avg = result._1 / result._2
        outVector.putDouble(0, avg)

        Iterator(new ColumnarBatch(Array(outVector), 1))
      }
  }

  override def output: Seq[Attribute] = Seq(
      AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
