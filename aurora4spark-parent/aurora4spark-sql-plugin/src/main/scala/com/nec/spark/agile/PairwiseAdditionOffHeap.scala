package com.nec.spark.agile

import com.nec.{AvgSimple, SumPairwise, VeJavaContext}
import com.nec.aurora.Aurora
import com.nec.spark.Aurora4SparkExecutorPlugin._veo_proc
import com.nec.spark.agile.PairwiseAdditionOffHeap.OffHeapPairwiseSummer
import com.nec.spark.agile.SingleValueStubPlan.SparkDefaultColumnName

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import sun.misc.Unsafe
import java.nio.file.Path

import com.nec.spark.Aurora4SparkExecutorPlugin

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
            val doubleA = getUnsafe.getDouble(memoryLocationA + i * 8)
            val doubleB = getUnsafe.getDouble(memoryLocationB + i * 8)

            val result = doubleA + doubleB
            getUnsafe.putDouble(memoryLocationOut + i * 8, result)
          }
      }
    }

    case object VeoBased extends OffHeapPairwiseSummer {
      def sum(
        memoryLocationA: Long,
        memoryLocationB: Long,
        memoryLocationOut: Long,
        count: Int
      ): Unit = {
          val vej =
            new VeJavaContext(Aurora4SparkExecutorPlugin._veo_ctx, Aurora4SparkExecutorPlugin.lib)
          SumPairwise.pairwise_sum_doubles_mem(
            vej, memoryLocationA, memoryLocationB, memoryLocationOut, count
          )
        }
    }
  }

}
case class PairwiseAdditionOffHeap(child: SparkPlan, summer: OffHeapPairwiseSummer)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
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
