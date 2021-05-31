package com.nec.spark.planning

import com.nec.spark.agile.{OutputColumn, OutputColumnAggregated}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ArrowGenericAggregationPlanOffHeap(child: SparkPlan,
                                              outputColumns: Seq[OutputColumn]
                                             ) extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .mapPartitions { it =>
        val columnarBatch = it.toList
        val offHeapAggregations = outputColumns.map {

          case OutputColumn(inputColumns, outputColumnIndex, columnAggregation, outputAggregator) => {
            val ra = new RootAllocator()
            val dataVectors = inputColumns
              .map(column => {
                val columns = columnarBatch.flatMap(
                  batch => batch.column(column.index)
                    .asInstanceOf[OffHeapColumnVector]
                    .getDoubles(0, batch.numRows())
                )
                columns
              })
            val aggregationResults = dataVectors.map(elems => {
              val vector = new Float8Vector("value", ra)
              elems.zipWithIndex.foreach{
                case(elem, idx) => vector.setSafe(idx, elem)
              }
              vector.setValueCount(elems.size)
              outputAggregator.aggregateOffHeap(vector)
            })

            OutputColumnAggregated(outputColumnIndex,
              columnAggregation,
              aggregationResults,
              dataVectors.size
            )
          }
        }
        offHeapAggregations.toIterator
      }
      .coalesce(1)
      .mapPartitions(its => {

        val elementsSum = its.toList.sortBy(_.outputColumnIndex).map {
          case OutputColumnAggregated(outIndex, columnAggregation, columns, _) => columnAggregation.aggregate(columns)
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
