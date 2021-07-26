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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
import sun.misc.Unsafe

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

  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .map { columnarBatch =>
        val vector = columnarBatch.column(column.index).asInstanceOf[OffHeapColumnVector]
        summer.sum(vector.valuesNativeAddress(), columnarBatch.numRows())
      }
  }
    .coalesce(1)
    .mapPartitions(its => {

      val result = its.reduce((a, b) => a + b)
      val offHeapVector = new OffHeapColumnVector(1, DoubleType)
      offHeapVector.putDouble(0, result)

      Iterator(new ColumnarBatch(Array(offHeapVector)))
    })

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
