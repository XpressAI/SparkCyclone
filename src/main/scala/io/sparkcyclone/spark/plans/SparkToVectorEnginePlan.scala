package io.sparkcyclone.spark.plans

import io.sparkcyclone.data.ColumnBatchEncoding
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.transfer.{BpcvTransferDescriptor, RowCollectingTransferDescriptor}
import io.sparkcyclone.data.vector._
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.spark.transformation._
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.vectorengine.{LibCyclone, LibraryReference}
import scala.collection.mutable.{Buffer => MSeq}
import scala.util.Try
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan, UnaryExecNode}

final case class SparkToVectorEnginePlan(val child: SparkPlan,
                                         sortOrder: Option[Seq[SortOrder]] = None)
                                         extends UnaryExecNode
                                         with LazyLogging
                                         with SupportsVeColBatch
                                         with RowToColumnarTransition
                                         with PlanMetrics {

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT) ++ invocationMetrics("Conversion") ++ invocationMetrics("Materialization") ++ batchMetrics("byte")

  override protected def doCanonicalize: SparkPlan = {
    super.doCanonicalize
  }

  override def output: Seq[Attribute] = {
    child.output
  }

  override def outputOrdering: Seq[SortOrder] = {
    sortOrder.getOrElse(Seq.empty)
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    Seq(sortOrder.map(OrderedDistribution(_)).getOrElse(UnspecifiedDistribution))
  }

  override def dataCleanup: DataCleanup = {
    DataCleanup.cleanup(getClass)
  }

  private[plans] def executeFromColInput: RDD[VeColBatch] = {
    val encoding = ColumnBatchEncoding.fromConf(conf)(sparkContext)

    child.executeColumnar.mapPartitions { colbatches =>
      val schema = encoding.makeArrowSchema(child.output)
      val dbuilder = new BpcvTransferDescriptor.Builder()
      val oldbatches = MSeq.empty[VeColBatch]

      withInvocationMetrics(PLAN) {
        colbatches.map(collectBatchMetrics(INPUT, _)).foreach {
          case WrappedColumnarBatch(wrapped: BytePointerColBatch) =>
            logger.debug(s"Got a BytePointerColBatch (rows = ${wrapped.numRows})")
            dbuilder.newBatch().addColumns(wrapped.columns)

          case WrappedColumnarBatch(wrapped: ByteArrayColBatch) =>
            logger.debug(s"Got a ByteArrayColBatch (rows = ${wrapped.numRows})")
            dbuilder.newBatch().addColumns(wrapped.toBytePointerColBatch.columns)

          case WrappedColumnarBatch(wrapped: VeColBatch) =>
            logger.debug(s"Got a VeColBatch (rows = ${wrapped.numRows})")
            oldbatches += wrapped

          case WrappedColumnarBatch(other) =>
            sys.error(s"WrappedColumnarBatch[${other.getClass.getSimpleName}] is currently not supported")

          case colbatch =>
            logger.debug(s"Got a Spark BatchColumnar (rows = ${colbatch.numRows})")
            dbuilder.newBatch().addColumns(colbatch.toBytePointerColBatch(schema).columns)
        }

        val descriptor = withInvocationMetrics("Conversion") {
          dbuilder.build
        }

        val newbatches = if (descriptor.nonEmpty) {
          Seq(withInvocationMetrics(VE) { vectorEngine.executeTransfer(descriptor) })
        } else {
          Seq.empty
        }

        collectBatchMetrics(OUTPUT, (oldbatches ++ newbatches).iterator)
      }
    }
  }

  private[plans] def executeFromRowInput: RDD[VeColBatch] = {
    val targetBatchSize = ColumnBatchEncoding.fromConf(conf)(sparkContext).targetNumRows

    child.execute.mapPartitions { rows =>
      new Iterator[VeColBatch] {
        private val maxRows = targetBatchSize

        override def hasNext: Boolean = {
          rows.hasNext
        }

        override def next: VeColBatch = {
          withInvocationMetrics(PLAN) {
            var currentRowCount = 0
            val descriptor = RowCollectingTransferDescriptor(child.output, targetBatchSize)

            withInvocationMetrics("Materialization") {
              while (rows.hasNext && currentRowCount < targetBatchSize) {
                descriptor.append(rows.next)
                currentRowCount += 1
              }
            }

            withInvocationMetrics("Conversion") {
              descriptor.buffer
            }

            val batch = withInvocationMetrics(VE) {
              vectorEngine.executeTransfer(descriptor)
            }

            collectBatchMetrics(OUTPUT, batch)
          }
        }
      }
    }
  }

  override def executeVeColumnar: RDD[VeColBatch] = {
    require(! child.isInstanceOf[SupportsVeColBatch], "Child plan should not be a VE plan")
    initializeMetrics()

    if (child.supportsColumnar) {
      executeFromColInput
    } else {
      executeFromRowInput
    }
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkToVectorEnginePlan = {
    copy(child = newChild)
  }
}
