package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin.cleanUpIfNotCached
import com.nec.spark.planning.SupportsVeColBatch.logger
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
    ): Unit = logger.debug(
      s"Not cleaning up data at ${processId} / ${veColBatch.underlying.cols
        .map(_.containerLocation)} - from ${originalCallingContext.fullName.value}#${originalCallingContext.line.value}, directed by ${parent.getCanonicalName}"
    )
  }
  def cleanup(parent: Class[_]): DataCleanup = new DataCleanup {
    override def cleanup(veColBatch: VeColBatch)(implicit
      veProcess: VeProcess,
      processId: VeColVectorSource,
      originalCallingContext: OriginalCallingContext
    ): Unit = {
      logger.debug(
        s"Requesting to clean up data at ${processId} by ${originalCallingContext.fullName.value}#${originalCallingContext.line.value}, directed by ${parent.getCanonicalName}"
      )
      cleanUpIfNotCached(veColBatch)
    }
  }

}
