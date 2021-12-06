package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.VeColBatchConverters.BasedOnColumnarBatch
import com.nec.ve.VeColBatch
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, DualMode}

import scala.collection.JavaConverters.asScalaBufferConverter

object VeColBatchConverters {

  def getNumRows(sparkContext: SparkContext, conf: SQLConf) = {
    sparkContext.getConf
      .getOption("com.nec.spark.ve.columnBatchSize")
      .map(_.toInt)
      .getOrElse(conf.columnBatchSize)
  }

  object BasedOnColumnarBatch {
    object ColB {
      def unapply(internalRow: InternalRow): Option[VeColBatch] = ???
    }
  }

  def internalRowToVeColBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[VeColBatch] = {
    input.mapPartitions { iterator =>
      DualMode.handleIterator(iterator) match {
        case Left(colBatches) => colBatches
        case Right(rowIterator) =>
          if (rowIterator.hasNext) {
            lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
            new Iterator[VeColBatch] {
              private val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
              private val root = VectorSchemaRoot.create(arrowSchema, allocator)
              private val cb = new ColumnarBatch(
                root.getFieldVectors.asScala
                  .map(vector => new ArrowColumnVector(vector))
                  .toArray,
                root.getRowCount
              )

              private val arrowWriter = ArrowWriter.create(root)
              arrowWriter.finish()
              TaskContext.get().addTaskCompletionListener[Unit] { _ =>
                cb.close()
              }

              override def hasNext: Boolean = {
                rowIterator.hasNext
              }

              override def next(): VeColBatch = {
                arrowWriter.reset()
                cb.setNumRows(0)
                root.getFieldVectors.asScala.foreach(_.reset())
                var rowCount = 0
                while (rowCount < numRows && rowIterator.hasNext) {
                  val row = rowIterator.next()
                  arrowWriter.write(row)
                  arrowWriter.finish()
                  rowCount += 1
                }
                cb.setNumRows(rowCount)
                //              numInputRows += rowCount
                //              numOutputBatches += 1
                import SparkCycloneExecutorPlugin.veProcess
                try VeColBatch.fromColumnarBatch(cb)
                finally cb.close()
              }
            }
          } else {
            Iterator.empty
          }
      }
    }

  }

  case class SparkToVectorEngine(childPlan: SparkPlan)
    extends UnaryExecNode
    with Logging
    with SupportsVeColBatch {

    override protected def doCanonicalize(): SparkPlan = {
      super.doCanonicalize()
    }

    override def supportsColumnar: Boolean = true
    override def child = {
      // if (CacheManager.isCached(childPlan)) {
      // CachedVeRelation(childPlan.outputSet.toList, sparkContext.getExecutorMemoryStatus.size)
      // } else {
      childPlan
      // }
    }
    override def executeVeColumnar(): RDD[VeColBatch] = {
//      val numInputRows = longMetric("numInputRows")
//      val numOutputBatches = longMetric("numOutputBatches")
      // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
      // combine with some of the Arrow conversion tools we will need to unify some of the configs.
      val numRows: Int = getNumRows(sparkContext, conf)
      logInfo(s"Will make batches of ${numRows} rows...")
      val timeZoneId = conf.sessionLocalTimeZone

      child match {
        case v: SupportsVeColBatch => v.executeVeColumnar()
        case _ =>
          internalRowToVeColBatch(child.execute(), timeZoneId, child.schema, numRows)
      }
    }

    override def output: Seq[Attribute] = child.output
  }

  case class VectorEngineToSpark(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true

    override def doExecute(): RDD[InternalRow] =
      doExecuteColumnar().mapPartitions(columnarBatchIterator =>
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
              try veColBatch.toArrowColumnarBatch()
              finally veColBatch.cols.foreach(_.free())
            }
        }
    }

    override def output: Seq[Attribute] = child.output

  }
}
