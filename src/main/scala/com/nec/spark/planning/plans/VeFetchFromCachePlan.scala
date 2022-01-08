package com.nec.spark.planning.plans

import com.nec.arrow.colvector.ByteArrayColVector
import com.nec.cache.VeColColumnarVector
import com.nec.spark.planning.SupportsVeColBatch
import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VeFetchFromCachePlan(child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging {

  private def unwrapBatch(
    columnarBatch: ColumnarBatch
  ): List[Either[VeColVector, ByteArrayColVector]] =
    (0 until columnarBatch.numCols())
      .map(colIdx => columnarBatch.column(colIdx).asInstanceOf[VeColColumnarVector].dualVeBatch)
      .toList

  override def executeVeColumnar(): RDD[VeColBatch] = child
    .executeColumnar()
    .map(cb => {
      logger.debug(s"Mapping ColumnarBatch ${cb} to VE")
      import com.nec.spark.SparkCycloneExecutorPlugin._
      import OriginalCallingContext.Automatic._

      val res = VeColBatch.fromList(unwrapBatch(cb).map {
        case Left(veColVector) => veColVector
        case Right(byteArrayColVector) =>
          val colVec = byteArrayColVector.transferToByteBuffers().toVeColVector()

          /* If we derived it from the byte-array cache, then clean up the inputs at the end.
           * For VE-cached data, don't clean it up - this is done by the Executor instead. */
          import OriginalCallingContext.Automatic._

          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            colVec.free()
          }

          colVec
      })
      logger.debug(s"Finished mapping ColumnarBatch ${cb} to VE: ${res}")
      res
    })

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: SupportsVeColBatch.DataCleanup = DataCleanup.noCleanup(this.getClass)
}
