package com.nec.spark

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.{DataType, DoubleType}

package object agile {
  case class AttributeName(value: String) extends AnyVal
  case class SparkPlanWithMetadata(sparkPlan: SparkPlan, attributes: Seq[Seq[AttributeName]])
  case class VeoSparkPlanWithMetadata(sparkPlan: SparkPlan, attributes: Seq[ColumnAggregation])

  type ColumnIndex = Int
  type ColumnWithNumbers = (ColumnIndex, Iterable[Double])

  def createProjectionForSeq(seqSize: Int): UnsafeProjection = {
    val types: Array[DataType] = Seq.fill(seqSize)(DoubleType).toArray
    UnsafeProjection.create(types)
  }

  sealed trait AggregateOperation
  case object Addition extends AggregateOperation
  case object Subtraction extends AggregateOperation
  case object NoAggregation extends AggregateOperation
  case class ColumnAggregation(columns: Seq[Column], aggregation: AggregateOperation,
                               columnIndex: ColumnIndex)
  case class Column(index: Int, name: String)
  case class DataColumnAggregation(outputColumnIndex: ColumnIndex,
                                      aggregation: AggregateOperation,
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

}
