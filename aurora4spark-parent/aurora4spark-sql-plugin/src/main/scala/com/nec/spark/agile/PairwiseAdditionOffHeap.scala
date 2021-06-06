package com.nec.spark.agile

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.functions.AddPairwise
import com.nec.older.SumPairwise
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.ve.VeJavaContext
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import sun.misc.Unsafe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

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
      .mapPartitions { partitionInternalRows =>
        Iterator
          .continually {
            val timeZoneId = conf.sessionLocalTimeZone
            val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
              s"writer for pairwise addition",
              0,
              Long.MaxValue
            )
            val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
            val root = VectorSchemaRoot.create(arrowSchema, allocator)
            val arrowWriter = ArrowWriter.create(root)
            import scala.collection.JavaConverters._
            partitionInternalRows.foreach { cb =>
              cb.rowIterator().asScala.foreach { ir => arrowWriter.write(ir) }
            }
            arrowWriter.finish()

            val outputVector = new Float8Vector("result", ArrowUtilsExposed.rootAllocator)

            AddPairwise.runOn(arrowInterface)(
              root.getVector(0).asInstanceOf[Float8Vector],
              root.getVector(1).asInstanceOf[Float8Vector],
              outputVector
            )

            val cb = new ColumnarBatch(Array(new ArrowColumnVector(outputVector)))
            cb.setNumRows(root.getRowCount)
            cb
          }
          .take(1)
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
