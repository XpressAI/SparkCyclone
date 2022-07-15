package io.sparkcyclone.spark.plans

import io.sparkcyclone.data.ColumnBatchEncoding
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.transfer.{BpcvTransferDescriptor, RowCollectingTransferDescriptor}
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.spark.transformation._
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.vectorengine.{LibCyclone, LibraryReference}
import scala.util.Try
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan, UnaryExecNode}

final case class SparkToVectorEnginePlan(val child: SparkPlan,
                                         val veFunction: VeFunction,
                                         sortOrder: Option[Seq[SortOrder]] = None) extends UnaryExecNode
                                                                                    with LazyLogging
                                                                                    with SupportsVeColBatch
                                                                                    with PlanCallsVeFunction
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

  private[plans] def loadLibCyclone: LibraryReference = {
    /*
      The veFunction is used to locate and load the Cyclone C++ library, so that
      `handle_transfer` can be called.

      TODO: Find a better way of calling a library function ("handle_transfer") from here
    */
    veProcess.load(veFunction.libraryPath.getParent.resolve("sources").resolve(LibCyclone.FileName))
  }

  private[plans] def executeFromColInput: RDD[VeColBatch] = {
    val encoding = ColumnBatchEncoding.fromConf(conf)(sparkContext)

    child.executeColumnar.mapPartitions { colbatches =>
      val schema = encoding.makeArrowSchema(child.output)

      withInvocationMetrics(PLAN) {
        val descriptor = withInvocationMetrics("Conversion") {
          collectBatchMetrics(INPUT, colbatches)
            .foldLeft(new BpcvTransferDescriptor.Builder()) { case (builder, colbatch) =>
              builder.newBatch().addColumns(colbatch.toBytePointerColBatch(schema).columns)
            }
            .build()
        }

        collectBatchMetrics(OUTPUT, if (descriptor.isEmpty) {
          logger.debug("Empty transfer descriptor")
          Iterator.empty

        } else {
          val batch = withInvocationMetrics(VE) {
            vectorEngine.executeTransfer(loadLibCyclone, descriptor)
          }
          Seq(batch).iterator
        })
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
              vectorEngine.executeTransfer(loadLibCyclone, descriptor)
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
    val byteTotalBatchRowCount = longMetric(s"byteTotalBatchRowCount")

    if (child.supportsColumnar) {
      executeFromColInput
    } else {
      executeFromRowInput
    }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(veFunction = f(veFunction))
}
