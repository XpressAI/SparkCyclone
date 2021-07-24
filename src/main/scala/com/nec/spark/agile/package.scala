package com.nec.spark

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.functions.{Avg, Sum}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.DoubleType
package object agile {
  case class AttributeName(value: String) extends AnyVal
  case class SingleColumnSparkPlan(sparkPlan: SparkPlan, column: Column)
  case class PartialSingleColumnSparkPlan(plans: Seq[SparkPlan], parent: SparkPlan, child: SparkPlan, column: Column) {
    def replaceMain(replaceWith: SparkPlan): SparkPlan = {
      plans.reverse.dropWhile(plan => plan != parent)
        .foldLeft(replaceWith){
          case (plan, f@HashAggregateExec(_, _, _, _, _, _, _)) => {
            f.copy(child=plan)
          }
          case (plan, f@ShuffleExchangeExec(_, _, _)) => {
            f.copy(child=plan)
          }
        }
      }
    }

  case class GenericSparkPlanDescription(
    sparkPlan: SparkPlan,
    outColumns: Seq[OutputColumnPlanDescription]
  )

  type ColumnIndex = Int
  type ColumnWithNumbers = (ColumnIndex, Iterable[Double])

  sealed trait ColumnAggregator extends Serializable {
    def aggregate(inputData: Seq[Double]): Double
  }

  case class AdditionAggregator(interface: ArrowNativeInterfaceNumeric) extends ColumnAggregator {
    override def aggregate(inputData: Seq[Double]): Double = {
      val rootAllocator = new RootAllocator(Long.MaxValue)
      val vector = new Float8Vector("value", rootAllocator)
      vector.allocateNew()
      inputData.zipWithIndex
        .foreach { case (elem, idx) =>
          vector.setSafe(idx, elem)
        }
      vector.setValueCount(inputData.size)

      Sum.runOn(interface)(vector, 1).head
    }
  }

  case class SubtractionAggregator(subtractor: MultipleColumnsOffHeapSubtractor)
    extends ColumnAggregator {
    override def aggregate(inputData: Seq[Double]): Double = {
      val vector = new OffHeapColumnVector(inputData.size, DoubleType)
      inputData.zipWithIndex.foreach { case (elem, idx) =>
        vector.putDouble(idx, elem)
      }

      subtractor.subtract(vector.valuesNativeAddress(), inputData.size)
    }
  }

  case object NoAggregationAggregator extends ColumnAggregator {
    override def aggregate(inputData: Seq[Double]): Double = {
      if (inputData.size != 1) {
        throw new RuntimeException("Multiple columns passed to function without aggregation.")
      }
      inputData.head
    }
  }

  case class ColumnAggregationExpression(
    columns: Seq[Column],
    aggregation: Expression,
    columnIndex: ColumnIndex
  ) extends Serializable

  case class Column(index: Int, name: String) extends Serializable

  case class OutputColumnAggregated(
    outputColumnIndex: ColumnIndex,
    aggregation: ColumnAggregator,
    columns: Seq[Double],
    numberOfRows: Int
  ) {
    def combine(
      a: OutputColumnAggregated
    )(mergeFunc: (Double, Double) => Double): OutputColumnAggregated = {
      val columnsCombined = if (a.outputColumnIndex != this.outputColumnIndex) {
        throw new RuntimeException("Can't combine different output columns!")
      } else {
        a.columns.zip(this.columns).map { case (a, b) =>
          mergeFunc(a, b)
        }
      }

      this.copy(columns = columnsCombined, numberOfRows = (a.numberOfRows + this.numberOfRows))
    }
  }

  case class OutputColumn(
    inputColumns: Seq[Column],
    outputColumnIndex: ColumnIndex,
    columnAggregation: ColumnAggregator,
    outputAggregator: Aggregator
  ) extends Serializable

  case class OutputColumnPlanDescription(
                                          inputColumns: Seq[Column],
                                          outputColumnIndex: ColumnIndex,
                                          columnAggregation: Expression,
                                          outputAggregator: AggregateFunction
  ) extends Serializable

  trait Aggregator extends Serializable {
    def aggregateOffHeap(inputVector: Float8Vector): Double
  }

  class SumAggregator(interface: ArrowNativeInterfaceNumeric) extends Aggregator {
    override def aggregateOffHeap(inputVector: Float8Vector): Double = {
      Sum.runOn(interface)(inputVector, 1).head
    }
  }

  class AvgAggregator(interface: ArrowNativeInterfaceNumeric) extends Aggregator {
    override def aggregateOffHeap(inputVector: Float8Vector): Double = {
      Avg.runOn(interface)(inputVector, 1).head
    }
  }
}
