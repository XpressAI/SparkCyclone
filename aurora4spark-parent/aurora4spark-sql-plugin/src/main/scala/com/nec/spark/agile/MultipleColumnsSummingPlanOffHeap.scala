package com.nec.spark.agile

import com.nec.{SumSimple, VeJavaContext}
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.agile.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
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

    object VeoBased extends MultipleColumnsOffHeapSummer {

      override def sum(inputMemoryAddress: Long, count: Int): Double = {
        val vej =
          new VeJavaContext(Aurora4SparkExecutorPlugin._veo_ctx, Aurora4SparkExecutorPlugin.lib)
        SumSimple.sum_doubles_memory(vej, inputMemoryAddress, count)
      }
    }

  }
}

case class MultipleColumnsSummingPlanOffHeap(
  child: SparkPlan,
  summer: MultipleColumnsOffHeapSummer,
  columnarAggregations: Seq[ColumnAggregation]
) extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .map { columnarBatch =>
       val offHeapAggregations = columnarAggregations.map{
          case ColumnAggregation(columns, aggregationOperation, outputColumnIndex) => {
            val dataVectors = columns
              .map(column => columnarBatch.column(column.index).asInstanceOf[OffHeapColumnVector])
              .map(vector => summer.sum(vector.valuesNativeAddress(), columnarBatch.numRows()))

            OutputColumnWithData(outputColumnIndex,
              aggregationOperation, dataVectors, columnarBatch.numRows())
          }
        }
        offHeapAggregations
      }
      .coalesce(1)
      .mapPartitions(its => {

        val aggregated = its.toList.flatten.groupBy(_.outputColumnIndex).map {
          case (idx, columnAggregations) => columnAggregations.reduce((a, b) => a.combine(b)(_ + _))
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
