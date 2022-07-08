package io.sparkcyclone.spark.planning.plans

import io.sparkcyclone.cache.{CycloneCacheBase, VeColColumnarVector}
import io.sparkcyclone.data.vector.{ByteArrayColVector, VeColBatch, VeColVector}
import io.sparkcyclone.spark.planning.{DataCleanup, PlanMetrics, SupportsVeColBatch}
import io.sparkcyclone.util.CallContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

object VeFetchFromCachePlan {
  def apply(child: SparkPlan, serializer: CycloneCacheBase): VeFetchFromCachePlan =
    VeFetchFromCachePlan(child, requiresCleanup = serializer.requiresCleanUp)
}

case class VeFetchFromCachePlan(child: SparkPlan, requiresCleanup: Boolean)
  extends UnaryExecNode
  with SupportsVeColBatch
  with PlanMetrics
  with LazyLogging {

  override lazy val metrics = invocationMetrics(BATCH) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  private def unwrapBatch(
    columnarBatch: ColumnarBatch
  ): List[Either[VeColVector, ByteArrayColVector]] =
    (0 until columnarBatch.numCols())
      .map(colIdx => columnarBatch.column(colIdx).asInstanceOf[VeColColumnarVector].dualVeBatch)
      .toList

  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

    child
      .executeColumnar()
      .map(cb => {
        logger.debug(s"Mapping ColumnarBatch ${cb} to VE")
        import io.sparkcyclone.util.CallContextOps._
        import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
        collectBatchMetrics(INPUT, cb)

        withInvocationMetrics(BATCH){
          val res = VeColBatch(unwrapBatch(cb).map {
            case Left(veColVector)  => veColVector
            case Right(baColVector) => baColVector.toVeColVector
          })
          logger.debug(s"Finished mapping ColumnarBatch ${cb} to VE: ${res}")
          collectBatchMetrics(OUTPUT, res)
        }
      })
  }

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = {
    if (requiresCleanup) DataCleanup.cleanup(this.getClass)
    else DataCleanup.noCleanup(this.getClass)
  }
}
