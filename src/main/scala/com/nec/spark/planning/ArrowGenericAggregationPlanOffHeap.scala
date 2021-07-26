package com.nec.spark.planning

import com.nec.spark.agile.{OutputColumnAggregated, OutputColumn}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, Float8Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ArrowGenericAggregationPlanOffHeap(child: SparkPlan, outputColumns: Seq[OutputColumn])
  extends SparkPlan {

  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .execute()
      .mapPartitions { it =>
        val timeZoneId = conf.sessionLocalTimeZone
        val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
          s"writer for generic aggregation",
          0,
          Long.MaxValue
        )
        val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val arrowWriter = ArrowWriter.create(root)
        it.foreach(row => arrowWriter.write(row))
        arrowWriter.finish()
        outputColumns.map {
          case OutputColumn(
                inputColumns,
                outputColumnIndex,
                columnAggregation,
                outputAggregator
              ) => {
            val results = inputColumns
              .map(col => root.getFieldVectors.get(col.index).asInstanceOf[Float8Vector])
              .map(vector => outputAggregator.aggregateOffHeap(vector))

            OutputColumnAggregated(outputColumnIndex, columnAggregation, results, root.getRowCount)
          }
        }.toIterator
      }
      .coalesce(1)
      .mapPartitions(it => {
        val output = it.toList
          .groupBy(_.outputColumnIndex)
          .map { case (columnIndex, columns) =>
            columns.reduce((a, b) => a.combine(b)(_ + _))
          }

        val vectors = output.toList
          .sortBy(_.outputColumnIndex)
          .map {
            case OutputColumnAggregated(outputColumnIndex, aggregation, columns, numberOfRows) =>
              aggregation.aggregate(columns)
          }
          .map(result => {
            val vector = new OffHeapColumnVector(1, DoubleType)
            vector.putDouble(0, result)
            vector
          })
        Iterator(new ColumnarBatch(vectors.toArray))
      })
  }

  override def output: Seq[Attribute] = outputColumns.map {
    case OutputColumn(inputColumns, outputColumnIndex, columnAggregation, outputAggregator) =>
      AttributeReference(name = "_" + outputColumnIndex, dataType = DoubleType, nullable = false)()
  }

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
