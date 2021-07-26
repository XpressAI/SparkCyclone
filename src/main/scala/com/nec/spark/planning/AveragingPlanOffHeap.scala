package com.nec.spark.planning

import com.nec.aurora.Aurora
import com.nec.older.AvgSimple
import com.nec.spark.Aurora4SparkExecutorPlugin._veo_proc
import com.nec.spark.agile.{Column, ColumnIndex}
import com.nec.spark.planning.SparkPortingUtils.PortedSparkPlan
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.ve.VeJavaContext
import sun.misc.Unsafe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch

object AveragingPlanOffHeap {

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
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(_veo_proc)
        try {
          val lib: Long = Aurora.veo_load_library(_veo_proc, ve_so_name)
          val vej = new VeJavaContext(_veo_proc, ctx, lib)
          AvgSimple.avg_doubles_mem(vej, memoryLocation, count)
        } finally Aurora.veo_context_close(ctx)
      }
    }
  }

}

case class AveragingPlanOffHeap(child: SparkPlan,
                                summer: MultipleColumnsOffHeapSummer,
                                column: Column)
  extends SparkPlan {


  def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .map { columnarBatch =>
        val theCol = columnarBatch.column(column.index).asInstanceOf[OffHeapColumnVector]
        (
          summer.sum(theCol.valuesNativeAddress(), columnarBatch.numRows()),
          columnarBatch.numRows()
        )
      }
      .coalesce(1)
      .mapPartitions { nums =>
        val sum = nums.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        val totalAvg = sum._1/sum._2
        val offHeapColumnVector = new OffHeapColumnVector(1, DoubleType)
        offHeapColumnVector.putDouble(0, totalAvg)
        Iterator(new ColumnarBatch(Array(offHeapColumnVector)))
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
