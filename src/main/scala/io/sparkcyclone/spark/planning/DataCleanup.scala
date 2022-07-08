package io.sparkcyclone.spark.planning

import io.sparkcyclone.spark.SparkCycloneExecutorPlugin
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.vectorengine.VeProcess
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.data.VeColVectorSource
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
      SparkCycloneExecutorPlugin.batchesCache.cleanupIfNotCached(veColBatch)
    }
  }

}
