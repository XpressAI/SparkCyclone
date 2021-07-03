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
   if (child.supportsColumnar) {
      child
        .executeColumnar()
        .mapPartitions { columnarBatches =>
          columnarBatches.map { colBatch =>
            val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
            val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
              s"writer for word count",
              0,
              Long.MaxValue
            )
            val root = VectorSchemaRoot.create(arrowSchema, allocator)
            val arrowWriter = ColumnarArrowWriter.create(root)

            arrowWriter.writeColumns(colBatch)


            try summer.sum(root.getVector(0).asInstanceOf[Float8Vector], 1)
            finally {
              import scala.collection.JavaConverters._
              root.getFieldVectors().asScala.foreach(_.close())
              allocator.close()
            }
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
