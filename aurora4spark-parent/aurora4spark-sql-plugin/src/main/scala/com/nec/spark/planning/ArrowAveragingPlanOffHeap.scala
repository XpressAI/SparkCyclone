package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Sum
import com.nec.spark.agile.{ColumnAggregation, OutputColumnAggregated}
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import com.nec.spark.agile.Column
import com.nec.spark.planning.ArrowSummingPlanOffHeap.OffHeapSummer
import org.apache.arrow.memory.RootAllocator

case class ArrowAveragingPlanOffHeap(child: SparkPlan, offHeapSummer: OffHeapSummer, column: Column)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .execute()
      .mapPartitions { rows =>
        val timeZoneId = conf.sessionLocalTimeZone
        val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
          s"writer for word count",
          0,
          Long.MaxValue
        )
        val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val arrowWriter = ArrowWriter.create(root)
        rows.foreach(row => arrowWriter.write(row))
        arrowWriter.finish()
        val vector = root.getVector(0).asInstanceOf[Float8Vector]
        arrowWriter.finish()

        Iterator((offHeapSummer.sum(vector, 1), vector.getValueCount))
      }
      .coalesce(1)
      .mapPartitions { it =>
        val result = it.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        val outVector = new OffHeapColumnVector(1, DoubleType)
        val avg = result._1 / result._2
        outVector.putDouble(0, avg)

        Iterator(new ColumnarBatch(Array(outVector), 1))
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
