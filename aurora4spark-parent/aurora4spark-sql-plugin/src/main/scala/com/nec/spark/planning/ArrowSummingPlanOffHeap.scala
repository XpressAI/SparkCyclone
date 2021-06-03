package com.nec.spark.planning

import com.nec.arrow.{
  ArrowNativeInterfaceNumeric,
  CArrowNativeInterfaceNumeric,
  VeArrowNativeInterfaceNumeric
}
import com.nec.arrow.functions.Sum
import com.nec.aurora.Aurora
import com.nec.older.SumSimple
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.agile.Column
import com.nec.spark.planning.SingleValueStubPlan.SparkDefaultColumnName
import com.nec.spark.planning.ArrowSummingPlanOffHeap.OffHeapSummer
import com.nec.ve.VeJavaContext
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
import sun.misc.Unsafe

import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.util.ArrowUtilsExposed

object ArrowSummingPlanOffHeap {

  trait OffHeapSummer extends Serializable {
    def sum(vector: Float8Vector, columnCount: Int): Double
  }

  object OffHeapSummer {

    case class CBased(libPath: String) extends OffHeapSummer {

      override def sum(vector: Float8Vector, columnCount: Int): Double = {
        Sum.runOn(new CArrowNativeInterfaceNumeric(libPath))(vector, columnCount).head
      }
    }

    case object VeoBased extends OffHeapSummer {
      override def sum(vector: Float8Vector, columnCount: Int): Double = {
        Sum
          .runOn(
            new VeArrowNativeInterfaceNumeric(
              Aurora4SparkExecutorPlugin._veo_proc,
              Aurora4SparkExecutorPlugin._veo_ctx,
              Aurora4SparkExecutorPlugin.lib
            )
          )(vector, columnCount)
          .head
      }
    }

  }
}

case class ArrowSummingPlanOffHeap(child: SparkPlan, summer: OffHeapSummer, column: Column)
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

        Iterator(summer.sum(root.getVector(0).asInstanceOf[Float8Vector], 1))
      }
      .coalesce(1)
      .mapPartitions { it =>
        val result = it.reduce((a, b) => a + b)
        val outVector = new OffHeapColumnVector(1, DoubleType)
        outVector.putDouble(0, result)

        Iterator(new ColumnarBatch(Array(outVector), 1))
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
