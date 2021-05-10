package com.nec.spark.agile

import com.nec.{SumPairwise, SumSimple, VeJavaContext}
import com.nec.aurora.Aurora
import com.nec.spark.agile.SingleValueStubPlan.SparkDefaultColumnName
import com.nec.spark.agile.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import sun.misc.Unsafe

object MultipleColumnsSummingPlanOffHeap {

  trait MultipleColumnsOffHeapSummer extends Serializable {
    def sum(inputMemoryAddress: Long, count: Int): Double
  }

  object MultipleColumnsOffHeapSummer {

    object UnsafeBased extends MultipleColumnsOffHeapSummer {
      private def getUnsafe: Unsafe = {
        val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
        theUnsafe.setAccessible(true)
        theUnsafe.get(null).asInstanceOf[Unsafe]
      }

      def sum(inputMemoryAddress: Long, count: Int): Double = {
        (0 until count)
          .map(index => getUnsafe.getDouble(inputMemoryAddress + index * 8))
          .sum
      }
    }

    case class VeoBased(ve_so_name: String) extends MultipleColumnsOffHeapSummer {

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
            val vej = new VeJavaContext(ctx, lib)
            SumSimple.sum_doubles_memory(vej, inputMemoryAddress, count)
          } finally Aurora.veo_context_close(ctx)
        } finally Aurora.veo_proc_destroy(proc)
      }
    }

  }
}

case class MultipleColumnsSummingPlanOffHeap(child: RowToColumnarExec,
                                             summer: MultipleColumnsOffHeapSummer,
                                             attributesMapping: Seq[Seq[Int]]) extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .doExecuteColumnar()
      .map { columnarBatch =>
        val columns = attributesMapping.zipWithIndex.map {
          case (mappings, idx) => mappings.map(colIndex =>
            (idx, columnarBatch.column(colIndex).asInstanceOf[OffHeapColumnVector])
          )
        }

        columns.flatten.map{
          case (columnIndex, vector) =>
            (columnIndex, summer.sum(vector.valuesNativeAddress(), columnarBatch.numRows()))
        }
      }
      .coalesce(1)
      .mapPartitions(its => {
        val elementsSum = its
          .toList
          .flatten
          .groupBy(_._1)
          .map {
            case (columnIndex, elements) => (columnIndex, elements.map(_._2).sum)
          }.toSeq
          .sortBy(_._1)   //Not entirely sure if we need to do this.


        val vectors = elementsSum.map(_  => new OnHeapColumnVector(1, DoubleType))

        elementsSum.zip(vectors).foreach{
          case((_, sum), vector) => vector.putDouble(0, sum)
        }


        Iterator(new ColumnarBatch(vectors.toArray, 1))
      })
  }

  override def output: Seq[Attribute] = attributesMapping.zipWithIndex.map{
    case(_, columnIndex) =>
      AttributeReference(name = "_" + columnIndex, dataType = DoubleType, nullable = false)()
  }

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
