package com.nec.spark.agile

import com.nec.{SumPairwise, VeJavaContext}
import com.nec.aurora.Aurora
import com.nec.spark.Aurora4SparkExecutorPlugin._veo_proc
import com.nec.spark.agile.PairwiseAdditionOffHeap.OffHeapPairwiseSummer
import com.nec.spark.agile.SingleValueStubPlan.SparkDefaultColumnName

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import sun.misc.Unsafe

object PairwiseAdditionOffHeap {

  trait OffHeapPairwiseSummer extends Serializable {
    def sum(memoryLocationA: Long, memoryLocationB: Long, memoryLocationOut: Long, count: Int): Unit
  }

  object OffHeapPairwiseSummer {
    object UnsafeBased extends OffHeapPairwiseSummer {
      private def getUnsafe: Unsafe = {
        val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
        theUnsafe.setAccessible(true)
        theUnsafe.get(null).asInstanceOf[Unsafe]
      }
      def sum(
        memoryLocationA: Long,
        memoryLocationB: Long,
        memoryLocationOut: Long,
        count: Int
      ): Unit = {
        (0 until count)
          .foreach { i =>
            getUnsafe.putDouble(
              memoryLocationOut + i * 8,
              getUnsafe.getDouble(memoryLocationA + i * 8) + getUnsafe.getDouble(
                memoryLocationB + i * 8
              )
            )
          }
      }
    }

    case class VeoBased(ve_so_name: String) extends OffHeapPairwiseSummer {
      def sum(
        memoryLocationA: Long,
        memoryLocationB: Long,
        memoryLocationOut: Long,
        count: Int
      ): Unit = {
        println(s"SO name: ${ve_so_name}")
        println(s"Reusing proc = ${_veo_proc}")
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(_veo_proc)
        try {
          println(s"Created ctx = ${ctx}")
          val lib: Long = Aurora.veo_load_library(_veo_proc, ve_so_name)
          println(s"Loaded lib = ${lib}")
          val vej = new VeJavaContext(ctx, lib)
          SumPairwise.pairwise_sum_doubles_mem(
            vej,
            memoryLocationA,
            memoryLocationB,
            memoryLocationOut,
            count
          )
        } finally Aurora.veo_context_close(ctx)
      }
    }
  }

}
case class PairwiseAdditionOffHeap(child: RowToColumnarExec, summer: OffHeapPairwiseSummer)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .doExecuteColumnar()
      .map { columnarBatch =>
        val colA = columnarBatch.column(0).asInstanceOf[OffHeapColumnVector]
        val colB = columnarBatch.column(1).asInstanceOf[OffHeapColumnVector]
        val outCvs = OffHeapColumnVector.allocateColumns(
          columnarBatch.numRows(),
          StructType(Array(StructField("value", DoubleType, nullable = false)))
        )

        summer.sum(
          colA.valuesNativeAddress(),
          colB.valuesNativeAddress(),
          outCvs.head.valuesNativeAddress(),
          columnarBatch.numRows()
        )

        new ColumnarBatch(outCvs.map(ov => ov: ColumnVector), columnarBatch.numRows())
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
