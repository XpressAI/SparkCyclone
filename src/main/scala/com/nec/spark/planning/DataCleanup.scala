package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin.cleanUpIfNotCached
import com.nec.colvector.VeColBatch
import com.nec.ve.VeProcess
import com.nec.util.CallContext
import com.nec.colvector.VeColVectorSource
import com.typesafe.scalalogging.LazyLogging

trait DataCleanup {
  def cleanup(veColBatch: VeColBatch)(implicit
    veProcess: VeProcess,
    processId: VeColVectorSource,
    context: CallContext
  ): Unit
}
object DataCleanup extends LazyLogging {
  def noCleanup(parent: Class[_]): DataCleanup = new DataCleanup {
    override def cleanup(veColBatch: VeColBatch)(implicit
      veProcess: VeProcess,
      processId: VeColVectorSource,
      context: CallContext
    ): Unit = logger.trace(
      s"Not cleaning up data at ${processId} / ${veColBatch.columns
        .map(_.container)} - from ${context.fullName.value}#${context.line.value}, directed by ${parent.getCanonicalName}"
    )
  }
  def cleanup(parent: Class[_]): DataCleanup = new DataCleanup {
    override def cleanup(veColBatch: VeColBatch)(implicit
      veProcess: VeProcess,
      processId: VeColVectorSource,
      context: CallContext
    ): Unit = {
      logger.trace(
        s"Requesting to clean up data of ${veColBatch.columns
          .map(_.container)} at ${processId} by ${context.fullName.value}#${context.line.value}, directed by ${parent.getCanonicalName}"
      )
      cleanUpIfNotCached(veColBatch)
    }
  }

}
