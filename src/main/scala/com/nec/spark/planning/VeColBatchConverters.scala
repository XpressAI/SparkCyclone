package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.ArrowColumnarToRowPlan.mapBatchToRow
import com.nec.spark.planning.RowToArrowColumnarPlan.collectInputRows
import com.nec.ve.VeColBatch
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.JavaConverters.asScalaBufferConverter

object VeColBatchConverters {

  case class SparkToVectorEngine(override val child: SparkPlan)
    extends UnaryExecNode
    with SupportsVeColBatch {
    override def supportsColumnar: Boolean = true

    override def executeVeColumnar(): RDD[VeColBatch] = {
      val timeZoneId = conf.sessionLocalTimeZone

      child
        .execute()
        .mapPartitions(rowIt => {
          lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)
          Iterator
            .continually {
              val root =
                collectInputRows(rowIt, ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId))

              import SparkCycloneExecutorPlugin.veProcess
              val cb = new ColumnarBatch(
                root.getFieldVectors.asScala
                  .map(vector => new ArrowColumnVector(vector))
                  .toArray,
                root.getRowCount
              )
              VeColBatch.fromColumnarBatch(cb)
            }
            .take(1)
        })

    }

    override def output: Seq[Attribute] = child.output
  }

  case class VectorEngineToSpark(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true

    override def doExecute(): RDD[InternalRow] =
      doExecuteColumnar.mapPartitions(columnarBatchIterator =>
        columnarBatchIterator.flatMap(ArrowColumnarToRowPlan.mapBatchToRow)
      )

    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
      child
        .asInstanceOf[SupportsVeColBatch]
        .executeVeColumnar()
        .mapPartitions { iterator =>
          import SparkCycloneExecutorPlugin.veProcess
          lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)

          iterator
            .map { veColBatch =>
              veColBatch.toArrowColumnarBatch()
            }
        }
    }

    override def output: Seq[Attribute] = child.output

  }
}
