package com.nec.spark.planning

import com.nec.aurora.Aurora
import com.nec.older.SumSimple
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.Aurora4SparkExecutorPlugin._veo_proc
import com.nec.spark.agile.Column
import com.nec.spark.planning.SparkPortingUtils.PortedSparkPlan
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.ve.VeJavaContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, UnsafeRow}
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
import sun.misc.Unsafe

import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}

object SummingPlanOffHeap {

  def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  trait MultipleColumnsOffHeapSummer extends Serializable {
    def sum(inputMemoryAddress: Long, count: Int): Double
  }

  object MultipleColumnsOffHeapSummer {

    object UnsafeBased extends MultipleColumnsOffHeapSummer {

      def sum(inputMemoryAddress: Long, count: Int): Double = {
        (0 until count)
          .map(index => getUnsafe.getDouble(inputMemoryAddress + index * 8))
          .sum
      }
    }

    object VeoBased extends MultipleColumnsOffHeapSummer {

      override def sum(inputMemoryAddress: Long, count: Int): Double = {
        val ctx = Aurora.veo_context_open(_veo_proc)
        try {
          val vej =
            new VeJavaContext(
              Aurora4SparkExecutorPlugin._veo_proc,
              ctx,
              Aurora4SparkExecutorPlugin.lib
            )
          SumSimple.sum_doubles_memory(vej, inputMemoryAddress, count)
        } finally Aurora.veo_context_close(ctx)
      }
    }

  }
}

case class SummingPlanOffHeap(
  child: SparkPlan,
  summer: MultipleColumnsOffHeapSummer,
  column: Column
) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    child
      .execute()
      .mapPartitions(rows => {
        val rowsList = rows.toList
        val offHeapVector = new OffHeapColumnVector(rowsList.size, DoubleType)
        rowsList
          .zipWithIndex
          .foreach {
            case (elem, idx) => offHeapVector.putDouble(idx, elem.getDouble(column.index))
          }

        Iterator(summer.sum(offHeapVector.valuesNativeAddress(), rowsList.size))
      })
      .coalesce(1)
      .mapPartitions(its => {
        val row = new UnsafeRow(1)
        val holder = new BufferHolder(row)
        val writer = new UnsafeRowWriter(holder, 1)
        holder.reset()
        val result = its.reduce((a, b) => a + b)
        writer.write(0, result)
        row.setTotalSize(holder.totalSize())
        Iterator(row)
      })
  }

    override def output: Seq[Attribute] = Seq(
      AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
    )

    override def children: Seq[SparkPlan] = Seq(child)

}
