package com.nec.spark.planning

import com.nec.arrow.functions.Sum
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.agile.Column
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.arrow.ColumnarArrowWriter
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

object ArrowSummingPlan {

  trait ArrowSummer extends Serializable {
    def sum(vector: Float8Vector, columnCount: Int): Double
  }

  object ArrowSummer {

    object JVMBased extends ArrowSummer {
      override def sum(vector: Float8Vector, columnCount: Int): Double = {
        Sum.sumJVM(vector, columnCount).head
      }
    }
    case class CBased(libPath: String) extends ArrowSummer {
      override def sum(vector: Float8Vector, columnCount: Int): Double = {
        Sum.runOn(new CArrowNativeInterfaceNumeric(libPath))(vector, columnCount).head
      }
    }

    case object VeoBased extends ArrowSummer {
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

case class ArrowSummingPlan(child: SparkPlan, summer: ArrowSummer, column: Column)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val timeZoneId = conf.sessionLocalTimeZone

    println(s"Supports columnar? ${child.supportsColumnar} ${child.getClass.getCanonicalName}")
    if (child.supportsColumnar) {
      child
        .executeColumnar()
        .mapPartitions { columnarBatches =>
          columnarBatches.map { colBatch =>
            0
          }
        }
        .coalesce(1)
        .mapPartitions { it =>
          val result = it.reduce((a, b) => a + b)
          val outVector = new OffHeapColumnVector(1, DoubleType)
          outVector.putDouble(0, 0)

          Iterator(new ColumnarBatch(Array(outVector), 1))
        }
    } else {
      child
        .execute()
        .mapPartitions { rows =>

          Iterator(0)
        }
        .coalesce(1)
        .mapPartitions { it =>
          val result = it.reduce((a, b) => a + b)
          val outVector = new OffHeapColumnVector(1, DoubleType)
          outVector.putDouble(0, result)

          Iterator(new ColumnarBatch(Array(outVector), 1))
        }
    }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
