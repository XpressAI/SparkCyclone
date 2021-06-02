package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Sum
import com.nec.aurora.Aurora
import com.nec.older.SumSimple
import com.nec.spark.agile.Column
import com.nec.spark.planning.SingleValueStubPlan.SparkDefaultColumnName
import com.nec.spark.planning.ArrowSummingPlanOffHeap.OffHeapSummer
import com.nec.ve.VeJavaContext
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

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

object ArrowSummingPlanOffHeap {

  trait OffHeapSummer extends Serializable {
    def sum(inputMemoryAddress: Long, count: Int): Double
  }

  object OffHeapSummer {

    object UnsafeBased extends OffHeapSummer {
      private def getUnsafe: Unsafe = {
        val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
        theUnsafe.setAccessible(true)
        theUnsafe.get(null).asInstanceOf[Unsafe]
      }

      def sum(inputMemoryAddress: Long, count: Int): Double = {
        println(count)
        (0 until count)
          .map(index => getUnsafe.getDouble(inputMemoryAddress + index * 8))
          .sum
      }
    }

    case class VeoBased(ve_so_name: String) extends OffHeapSummer {

      override def sum(inputMemoryAddress: Long, count: Int): Double = {
        println(s"SO name: ${ve_so_name}")
        val proc = Aurora.veo_proc_create(0)
        println(s"Created proc = ${proc}")
        try {
          val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
          println(s"Created ctx = ${ctx}")
          try {
            val lib: Long = Aurora.veo_load_library(proc, ve_so_name)
            println(s"Loaded lib = ${lib}")
            val vej = new VeJavaContext(proc, ctx, lib)
            SumSimple.sum_doubles_memory(vej, inputMemoryAddress, count)
          } finally Aurora.veo_context_close(ctx)
        } finally Aurora.veo_proc_destroy(proc)
      }
    }

  }
}

case class ArrowSummingPlanOffHeap(child: SparkPlan,
                                   nativeInterface: ArrowNativeInterfaceNumeric,
                                   column: Column) extends SparkPlan {

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

        Iterator(Sum.runOn(nativeInterface)(vector, 1).head)
      }
      .coalesce(1)
      .mapPartitions{  it =>
        val result = it.reduce((a, b) => a + b)
        val outVector = new OffHeapColumnVector(1, DoubleType)
        outVector.putDouble(0, result)

        Iterator(new ColumnarBatch(Array(outVector), 1))
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
