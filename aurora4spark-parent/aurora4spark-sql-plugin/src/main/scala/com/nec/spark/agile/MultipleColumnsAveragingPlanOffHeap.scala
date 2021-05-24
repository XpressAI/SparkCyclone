package com.nec.spark.agile

import com.nec.{AvgMultipleColumns, AvgSimple, SumPairwise, SumSimple, VeJavaContext}
import com.nec.aurora.Aurora
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.agile.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager
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

object MultipleColumnsAveragingPlanOffHeap {

  trait MultipleColumnsOffHeapAverager extends Serializable {
    def avg(inputMemoryAddress: Long, count: Int): Double
  }

  object MultipleColumnsOffHeapAverager {

    object UnsafeBased extends MultipleColumnsOffHeapAverager {
      private def getUnsafe: Unsafe = {
        val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
        theUnsafe.setAccessible(true)
        theUnsafe.get(null).asInstanceOf[Unsafe]
      }

      def avg(inputMemoryAddress: Long, count: Int): Double = {
        val sum = (0 until count)
          .map(index => getUnsafe.getDouble(inputMemoryAddress + index * 8))
          .sum

        sum / count
      }
    }

    case object VeoBased extends MultipleColumnsOffHeapAverager {

      override def avg(inputMemoryAddress: Long, count: Int): Double = {
        val vej =
          new VeJavaContext(Aurora4SparkExecutorPlugin._veo_ctx, Aurora4SparkExecutorPlugin.lib)
        AvgSimple.avg_doubles_mem(vej, inputMemoryAddress, count)
      }
    }

  }
}

case class MultipleColumnsAveragingPlanOffHeap (
                                                 child: RowToColumnarExec,
                                                 averager: MultipleColumnsOffHeapAverager,
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
              .map(vector => averager.avg(vector.valuesNativeAddress(), columnarBatch.numRows()))

            OutputColumnWithData(outputColumnIndex,
              aggregationOperation, dataVectors, columnarBatch.numRows())
          }
        }
        offHeapAggregations
      }
      .coalesce(1)
      .mapPartitions(its => {

        val aggregated = its.toList.flatten.groupBy(_.outputColumnIndex).map {
          case (idx, columnAggregations) =>
            columnAggregations.reduce((a, b) => a.combine(b)(_ + _))
        }

        val elementsSum = aggregated.toList.sortBy(_.outputColumnIndex).map {
          case OutputColumnWithData(outIndex, aggregator, columns, _) => aggregator.aggregate(columns)
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
