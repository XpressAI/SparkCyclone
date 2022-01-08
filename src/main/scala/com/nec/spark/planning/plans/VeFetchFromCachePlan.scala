package com.nec.spark.planning.plans

import com.nec.arrow.colvector.ByteArrayColVector
import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.spark.planning.{SupportsVeColBatch, VeCachedBatchSerializer, VeColColumnarVector}
import com.nec.ve.VeColBatch
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
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

      val res = VeColBatch.fromList(unwrapBatch(cb).map {
        case Left(veColVector)         => veColVector
        case Right(byteArrayColVector) => byteArrayColVector.transferToByteBuffers().toVeColVector()
      })
      logger.debug(s"Finished mapping ColumnarBatch ${cb} to VE: ${res}")
      res
    })

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: SupportsVeColBatch.DataCleanup = DataCleanup.NoCleanup
}
