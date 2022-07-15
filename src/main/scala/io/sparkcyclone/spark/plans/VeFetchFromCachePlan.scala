package io.sparkcyclone.spark.plans

import io.sparkcyclone.cache.CycloneCachedBatchSerializer
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.vector._
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.util.CallContextOps._
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

    child
      .executeColumnar
      .map { colbatch =>
        logger.debug(s"Mapping ColumnarBatch ${colbatch} to VE")
        collectBatchMetrics(INPUT, colbatch)
        withInvocationMetrics(BATCH) {
          val batch = VeColBatch(colbatch.columns.map(_.asInstanceOf[WrappedColumnVector].toVeColVector))
          logger.debug(s"Finished mapping ColumnarBatch ${colbatch} to VE: ${batch}")
          collectBatchMetrics(OUTPUT, batch)
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
}
