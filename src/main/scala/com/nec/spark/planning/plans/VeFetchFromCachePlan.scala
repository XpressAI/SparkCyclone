package com.nec.spark.planning.plans

import com.nec.cache.{CycloneCacheBase, VeColColumnarVector}
import com.nec.colvector.{ByteArrayColVector, VeColVector}
import com.nec.spark.planning.{DataCleanup, PlanMetrics, SupportsVeColBatch}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.concurrent.duration.NANOSECONDS

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
    child
      .executeColumnar()
      .map(cb => {
        logger.debug(s"Mapping ColumnarBatch ${cb} to VE")
        import OriginalCallingContext.Automatic._
        import com.nec.spark.SparkCycloneExecutorPlugin._
        collectBatchMetrics(INPUT, cb)

        withInvocationMetrics(BATCH){
          val res = VeColBatch.fromList(unwrapBatch(cb).map {
            case Left(veColVector)         => veColVector
            case Right(baColVector) =>
              import ImplicitMetrics._
              baColVector.toVeColVector
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
