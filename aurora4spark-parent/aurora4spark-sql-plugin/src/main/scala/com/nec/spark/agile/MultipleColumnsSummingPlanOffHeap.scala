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

case class MultipleColumnsSummingPlanOffHeap(
  child: RowToColumnarExec,
  summer: MultipleColumnsOffHeapSummer,
  columnarAggregations: Seq[ColumnAggregation]
) extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .doExecuteColumnar()
      .map { columnarBatch =>
       val offHeapAggregations = columnarAggregations.map{
          case ColumnAggregation(columns, aggregationOperation, outputColumnIndex) => {
            val dataVectors = columns
              .map(column => columnarBatch.column(column.index).asInstanceOf[OffHeapColumnVector])
              .map(vector => summer.sum(vector.valuesNativeAddress(), columnarBatch.numRows()))

            DataColumnAggregation(outputColumnIndex,
              aggregationOperation, dataVectors, columnarBatch.numRows())
          }
        }
        offHeapAggregations
      }
      .coalesce(1)
      .mapPartitions(its => {

        val aggregated = its.toList.flatten.groupBy(_.outputColumnIndex).map {
          case (idx, columnAggregations) => columnAggregations.reduce((a, b) => a.combine(b)(_+_))
        }

        val elementsSum = aggregated.toList.sortBy(_.outputColumnIndex).map {
          case DataColumnAggregation(outIndex, NoAggregation, columns, _) => columns.head
          case DataColumnAggregation(outIndex, Addition, columns, _) => columns.sum
          case DataColumnAggregation(outIndex, Subtraction, columns, _) =>
            columns.reduce((a, b) => a - b)
        }

        val vectors = elementsSum.map(_ => new OnHeapColumnVector(1, DoubleType))

        elementsSum.zip(vectors).foreach { case (sum, vector) =>
          vector.putDouble(0, sum)
        }

        Iterator(new ColumnarBatch(vectors.toArray, 1))
      })
  }

  override def output: Seq[Attribute] = columnarAggregations.zipWithIndex.map {
    case (_, columnIndex) =>
      AttributeReference(name = "_" + columnIndex, dataType = DoubleType, nullable = false)()
  }

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
