package com.nec.spark

import com.nec.spark.agile.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager
import com.nec.spark.agile.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.{DataType, DoubleType}

package object agile {
  case class AttributeName(value: String) extends AnyVal
  case class SparkPlanWithMetadata(sparkPlan: SparkPlan, attributes: Seq[Seq[AttributeName]])
  case class VeoSparkPlanWithMetadata(sparkPlan: SparkPlan, attributes: Seq[ColumnAggregation])
  case class GenericSparkPlanDescription(sparkPlan: SparkPlan,
                                         outColumns: Seq[OutputColumnPlanDescription])
  case class VeoGenericSparkPlan(sparkPlan: SparkPlan,
                                         outColumns: Seq[OutputColumn])
  type ColumnIndex = Int
  type ColumnWithNumbers = (ColumnIndex, Iterable[Double])

  def createProjectionForSeq(seqSize: Int): UnsafeProjection = {
    val types: Array[DataType] = Seq.fill(seqSize)(DoubleType).toArray
    UnsafeProjection.create(types)
  }

  sealed trait ColumnAggregateOperation extends Serializable
  case object Addition extends ColumnAggregateOperation
  case object Subtraction extends ColumnAggregateOperation
  case object NoAggregation extends ColumnAggregateOperation

  case class ColumnAggregation(columns: Seq[Column], aggregation: ColumnAggregateOperation,
                               columnIndex: ColumnIndex) extends Serializable
  case class Column(index: Int, name: String) extends Serializable

  case class DataColumnAggregation(outputColumnIndex: ColumnIndex,
                                   aggregation: ColumnAggregateOperation,
                                   columns: Seq[Double],
                                   numberOfRows: Int) {
    def combine(a: DataColumnAggregation)(mergeFunc: (Double, Double) => Double): DataColumnAggregation = {
      val columnsCombined = if(a.outputColumnIndex != this.outputColumnIndex) {
        throw new RuntimeException("Can't combine different output columns!")
      } else {
        a.columns.zip(this.columns).map{
          case (a, b) => mergeFunc(a, b)
        }
      }
      this.copy(columns = columnsCombined)
    }
  }

  case class OutputColumn(inputColumns: Seq[Column], outputColumnIndex: ColumnIndex,
                          columnAggregation: ColumnAggregateOperation,
                          outputAggregator: Aggregator) extends Serializable


  case class OutputColumnPlanDescription(inputColumns: Seq[Column], outputColumnIndex: ColumnIndex,
                                         columnAggregation: ColumnAggregateOperation,
                                         outputAggregator: AggregationFunction) extends Serializable

  sealed trait AggregationFunction
  case object SumAggregation extends AggregationFunction
  case object AvgAggregation extends AggregationFunction

  trait Aggregator extends Serializable {
    def aggregateOffHeap(memoryAddress: Long, inputSize: Int): Double
  }

  class SumAggregator(adder: MultipleColumnsOffHeapSummer) extends Aggregator {
    override def aggregateOffHeap(memoryAddress: Long, inputSize: ColumnIndex): Double =
      adder.sum(memoryAddress, inputSize)
  }

  class AvgAggregator(averager: MultipleColumnsOffHeapAverager) extends Aggregator {
    override def aggregateOffHeap(memoryAddress: Long, inputSize: ColumnIndex): Double =
      averager.avg(memoryAddress, inputSize)
  }
}
