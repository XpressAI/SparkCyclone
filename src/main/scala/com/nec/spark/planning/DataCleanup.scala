package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin.cleanUpIfNotCached
import com.nec.ve.{VeColBatch, VeProcess}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.typesafe.scalalogging.LazyLogging

trait DataCleanup {
  def cleanup(veColBatch: VeColBatch)(implicit
    veProcess: VeProcess,
    processId: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): Unit
}
object DataCleanup extends LazyLogging {
  def noCleanup(parent: Class[_]): DataCleanup = new DataCleanup {
    override def cleanup(veColBatch: VeColBatch)(implicit
      veProcess: VeProcess,
      processId: VeColVectorSource,
      originalCallingContext: OriginalCallingContext
    ): Unit = logger.trace(
      "Not cleaning up data at {} / {} - from {}#{}, directed by {}",
      processId, veColBatch.underlying.cols.map(_.containerLocation),
      originalCallingContext.fullName.value, originalCallingContext.line.value, parent.getCanonicalName
    )
  }
  def cleanup(parent: Class[_]): DataCleanup = new DataCleanup {
    override def cleanup(veColBatch: VeColBatch)(implicit
      veProcess: VeProcess,
      processId: VeColVectorSource,
      originalCallingContext: OriginalCallingContext
    ): Unit = {
      logger.trace(
        "Requesting to clean up data of {} at {} by {}#{}, directed by {}",
        veColBatch.underlying.cols.map(_.containerLocation), processId,
        originalCallingContext.fullName.value, originalCallingContext.line.value,
        parent.getCanonicalName
      )
      cleanUpIfNotCached(veColBatch)
    }
  }

}
