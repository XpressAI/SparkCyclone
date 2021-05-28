package com.nec.spark.planning

import com.nec.aurora.Aurora
import com.nec.older.AvgSimple
import com.nec.spark.Aurora4SparkExecutorPlugin._veo_proc
import com.nec.spark.planning.SingleValueStubPlan.SparkDefaultColumnName
import com.nec.spark.agile.ColumnIndex
import com.nec.spark.planning.AveragingSparkPlanOffHeap.OffHeapDoubleAverager
import com.nec.ve.VeJavaContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
import sun.misc.Unsafe

object AveragingSparkPlanOffHeap {

  trait OffHeapDoubleAverager extends Serializable {
    def average(memoryLocation: Long, count: Int): Double
  }

  object OffHeapDoubleAverager {
    object UnsafeBased extends OffHeapDoubleAverager {
      private def getUnsafe: Unsafe = {
        val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
        theUnsafe.setAccessible(true)
        theUnsafe.get(null).asInstanceOf[Unsafe]
      }
      override def average(memoryLocation: Long, count: ColumnIndex): Double = {
        (0 until count)
          .map { i =>
            getUnsafe.getDouble(memoryLocation + i * 8)
          }
          .toList
          .sum / count
      }
    }

    case class VeoBased(ve_so_name: String) extends OffHeapDoubleAverager {
      override def average(memoryLocation: Long, count: ColumnIndex): Double = {
        println(s"SO name: ${ve_so_name}")
        println(s"Reusing proc = ${_veo_proc}")
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(_veo_proc)
        println(s"Created ctx = ${ctx}")
        try {
          val lib: Long = Aurora.veo_load_library(_veo_proc, ve_so_name)
          val vej = new VeJavaContext(ctx, lib)
          AvgSimple.avg_doubles_mem(vej, memoryLocation, count)
        } finally Aurora.veo_context_close(ctx)
      }
    }
  }

}

case class AveragingSparkPlanOffHeap(child: RowToColumnarExec, averager: OffHeapDoubleAverager)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .doExecuteColumnar()
      .map { columnarBatch =>
        val theCol = columnarBatch.column(0).asInstanceOf[OffHeapColumnVector]
        (
          averager.average(theCol.valuesNativeAddress(), columnarBatch.numRows()),
          columnarBatch.numRows()
        )
      }
      .coalesce(1)
      .mapPartitions { nums =>
        Iterator {
          val nl = nums.toList
          val totalSize = nl.map(_._2).sum
          nl.map { case (avgs, gs) => avgs * (gs.toDouble / totalSize) }.sum
        }
      }
      .map { double =>
        val vector = new OnHeapColumnVector(1, DoubleType)
        vector.putDouble(0, double)
        new ColumnarBatch(Array(vector), 1)
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
