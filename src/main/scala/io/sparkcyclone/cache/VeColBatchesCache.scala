package io.sparkcyclone.cache

import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.spark.SparkCycloneExecutorPlugin.source
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.vectorengine.VeProcess
import scala.collection.concurrent.TrieMap
import com.typesafe.scalalogging.LazyLogging

class VeColBatchesCache extends LazyLogging {
  private[cache] val batches = TrieMap.empty[String, VeColBatch]

  def register(name: String, batch: VeColBatch): Unit = {
    batches += ((name, batch))
  }

  def cleanupIfNotCached(batch: VeColBatch)
                        (implicit process: VeProcess,
                         context: CallContext): Unit = {
    if (batches.values.toSeq.contains(batch)) {
      logger.trace(s"Data at ${batch.columns.map(_.container)} will not be cleaned up as it's cached: ${context}")

    } else {
      logger.trace(s"Will clean up data for ${batch.columns.map(_.container)}")
      batch.free
    }
  }

  def cleanup(implicit process: VeProcess): Unit = {
    import io.sparkcyclone.util.CallContextOps._
    batches.values.foreach(_.free())
    batches.clear
  }
}
