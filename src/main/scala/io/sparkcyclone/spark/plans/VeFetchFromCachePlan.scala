package io.sparkcyclone.spark.plans

import io.sparkcyclone.cache.CycloneCachedBatchSerializer
import io.sparkcyclone.data.ColumnBatchEncoding
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.transfer.BpcvTransferDescriptor
import io.sparkcyclone.data.vector._
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.util.CallContextOps._
import scala.collection.mutable.{Buffer => MSeq}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

object VeFetchFromCachePlan {
  def apply(child: SparkPlan, serializer: CycloneCachedBatchSerializer): VeFetchFromCachePlan =
    VeFetchFromCachePlan(child, requiresCleanup = serializer.requiresCleanUp)
}

case class VeFetchFromCachePlan(child: SparkPlan, requiresCleanup: Boolean)
  extends UnaryExecNode
  with SupportsVeColBatch
  with PlanMetrics
  with LazyLogging {

  override lazy val metrics = invocationMetrics(BATCH) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def executeVeColumnar: RDD[VeColBatch] = {
    initializeMetrics()
    val encoding = ColumnBatchEncoding.fromConf(conf)(sparkContext)

    child.executeColumnar.mapPartitions { colbatches =>
      val schema = encoding.makeArrowSchema(child.output)
      val dbuilder = new BpcvTransferDescriptor.Builder()
      val oldbatches = MSeq.empty[VeColBatch]

      withInvocationMetrics(PLAN) {
        colbatches.map(collectBatchMetrics(INPUT, _)).foreach {
          case WrappedColumnarBatch(wrapped: BytePointerColBatch) =>
            dbuilder.newBatch().addColumns(wrapped.columns)

          case WrappedColumnarBatch(wrapped: ByteArrayColBatch) =>
            dbuilder.newBatch().addColumns(wrapped.toBytePointerColBatch.columns)

          case WrappedColumnarBatch(wrapped: VeColBatch) =>
            oldbatches += wrapped

          case WrappedColumnarBatch(other) =>
            sys.error(s"WrappedColumnarBatch[${other.getClass.getSimpleName}] is currently not supported")

          case colbatch =>
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

  override def output: Seq[Attribute] = {
    child.output
  }

  override def dataCleanup: DataCleanup = {
    if (requiresCleanup) DataCleanup.cleanup(this.getClass)
    else DataCleanup.noCleanup(this.getClass)
  }

  override def withNewChildInternal(newChild: SparkPlan): VeFetchFromCachePlan = {
    copy(child = newChild)
  }
}
