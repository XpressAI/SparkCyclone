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
            val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
              s"writer for word count",
              0,
              Long.MaxValue
            )
            val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
            val vcv =
              arrowSchema.findField("value").createVector(allocator).asInstanceOf[Float8Vector]
            vcv.setValueCount(colBatch.numRows())
            val col = colBatch.column(0)
            var rowIdx = 0
            while (rowIdx < colBatch.numRows()) {
              vcv.set(rowIdx, col.getDouble(rowIdx))
              rowIdx = rowIdx + 1
            }
            summer.sum(vcv, 1)
          }
        }
        .coalesce(1)
        .mapPartitions { it =>
          val result = it.reduce((a, b) => a + b)
          val outVector = new OffHeapColumnVector(1, DoubleType)
          outVector.putDouble(0, result)

          Iterator(new ColumnarBatch(Array(outVector), 1))
        }
    } else if (false) {
      child
        .executeColumnar()
        .mapPartitions { columnarBatches =>
          val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
          val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
            s"writer for word count",
            0,
            Long.MaxValue
          )
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          val arrowWriter = ColumnarArrowWriter.create(root)

          columnarBatches.map { colBatch =>
            arrowWriter.writeColumns(colBatch)
            summer.sum(root.getVector(0).asInstanceOf[Float8Vector], 1)
          }
        }
        .coalesce(1)
        .mapPartitions { it =>
          val result = it.reduce((a, b) => a + b)
          val outVector = new OffHeapColumnVector(1, DoubleType)
          outVector.putDouble(0, result)

          Iterator(new ColumnarBatch(Array(outVector), 1))
        }
    } else {
      child
        .execute()
        .mapPartitions { rows =>
          val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
          val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
            s"writer for word count",
            0,
            Long.MaxValue
          )
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
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
