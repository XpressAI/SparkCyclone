package com.nec.spark.planning
import com.nec.spark.agile.OutputColumn
import com.nec.spark.agile.OutputColumnWithData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GenericAggregationPlanOffHeap(child: SparkPlan, outputColumns: Seq[OutputColumn])
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .flatMap { columnarBatch =>
        val offHeapAggregations = outputColumns.map {
          case OutputColumn(
                inputColumns,
                outputColumnIndex,
                columnAggregation,
                outputAggregator
              ) => {
            val dataVectors = inputColumns
              .map(column => columnarBatch.column(column.index).asInstanceOf[OffHeapColumnVector])
              .map(vector =>
                outputAggregator
                  .aggregateOffHeap(vector.valuesNativeAddress(), columnarBatch.numRows())
              )

            OutputColumnWithData(
              outputColumnIndex,
              columnAggregation,
              dataVectors,
              columnarBatch.numRows()
            )
          }
        }

        offHeapAggregations
      }
      .coalesce(1)
      .mapPartitions(its => {

        val elementsSum = its.toList.sortBy(_.outputColumnIndex).map {
          case OutputColumnWithData(outIndex, aggregator, columns, _) =>
            aggregator.aggregate(columns)
        }

        val vectors = elementsSum.map(_ => new OnHeapColumnVector(1, DoubleType))

        elementsSum.zip(vectors).foreach { case (sum, vector) =>
          vector.putDouble(0, sum)
        }

        Iterator(new ColumnarBatch(vectors.toArray, 1))
      })
  }

  override def output: Seq[Attribute] = outputColumns.map {
    case OutputColumn(inputColumns, outputColumnIndex, columnAggregation, outputAggregator) =>
      AttributeReference(name = "_" + outputColumnIndex, dataType = DoubleType, nullable = false)()
  }

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
