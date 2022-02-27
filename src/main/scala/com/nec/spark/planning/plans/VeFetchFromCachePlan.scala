package com.nec.spark.planning.plans

import com.nec.arrow.colvector.ByteArrayColVector
import com.nec.cache.{CycloneCacheBase, VeColColumnarVector}
import com.nec.spark.planning.{DataCleanup, SupportsVeColBatch}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.TaskContext
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
  with LazyLogging {

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time")
  )
  private def unwrapBatch(
    columnarBatch: ColumnarBatch
  ): List[Either[VeColVector, ByteArrayColVector]] =
    (0 until columnarBatch.numCols())
      .map(colIdx => columnarBatch.column(colIdx).asInstanceOf[VeColColumnarVector].dualVeBatch)
      .toList

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")
    val beforeExec = System.nanoTime()

    child
      .executeColumnar()
      .map(cb => {
        logger.debug(s"Mapping ColumnarBatch ${cb} to VE")
        import com.nec.spark.SparkCycloneExecutorPlugin._
        import OriginalCallingContext.Automatic._

        val res = VeColBatch.fromList(unwrapBatch(cb).map {
          case Left(veColVector)         => veColVector
          case Right(byteArrayColVector) => byteArrayColVector.transferToBytePointers().toVeColVector()
        })
        logger.debug(s"Finished mapping ColumnarBatch ${cb} to VE: ${res}")
        execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)
        res
      })
  }

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = {
    if (requiresCleanup) DataCleanup.cleanup(this.getClass)
    else DataCleanup.noCleanup(this.getClass)
  }
}
