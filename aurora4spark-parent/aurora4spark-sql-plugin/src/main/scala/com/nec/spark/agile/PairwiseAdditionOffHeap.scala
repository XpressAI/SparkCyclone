package com.nec.spark.agile

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Add
import com.nec.older.SumPairwise
import com.nec.spark.planning.SingleValueStubPlan.SparkDefaultColumnName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
import sun.misc.Unsafe
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.ve.VeJavaContext
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BaseFixedWidthVector
import org.apache.arrow.vector.Float8Vector

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
          new VeJavaContext(
            Aurora4SparkExecutorPlugin._veo_proc,
            Aurora4SparkExecutorPlugin._veo_ctx,
            Aurora4SparkExecutorPlugin.lib
          )
        SumPairwise.pairwise_sum_doubles_mem(
          vej,
          memoryLocationA,
          memoryLocationB,
          memoryLocationOut,
          count
        )
      }
    }
  }

}
case class PairwiseAdditionOffHeap(child: SparkPlan, arrowInterface: ArrowNativeInterfaceNumeric)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .map { columnarBatch =>
        val rootAllocator = new RootAllocator()
        val colA = columnarBatch.column(0).asInstanceOf[OffHeapColumnVector]
        val colB = columnarBatch.column(1).asInstanceOf[OffHeapColumnVector]

        val vectorA = new Float8Vector("value", rootAllocator)
        vectorA.setValueCount(columnarBatch.numRows())
        val field = classOf[BaseFixedWidthVector].getDeclaredField("valueBuffer")
        field.setAccessible(true)
        val firstBuf = new ArrowBuf(
          vectorA.getValidityBuffer.getReferenceManager,
          null,
          columnarBatch.numRows() * 8,
          colA.valuesNativeAddress()
        )
        field.set(vectorA, firstBuf)

        val vectorB = new Float8Vector("value", rootAllocator)
        vectorB.setValueCount(columnarBatch.numRows())
        field.setAccessible(true)
        val secondBuf = new ArrowBuf(
          vectorB.getValidityBuffer.getReferenceManager,
          null,
          columnarBatch.numRows() * 8,
          colB.valuesNativeAddress()
        )
        field.set(vectorB, secondBuf)

        val result = Add.runOn(arrowInterface)(vectorA, vectorB)

        val offHeapVector = new OffHeapColumnVector(columnarBatch.numRows(), DoubleType)

        result.zipWithIndex.foreach { case (elem, idx) =>
          offHeapVector.putDouble(idx, elem)
        }

        new ColumnarBatch(Array(offHeapVector), result.size)
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
