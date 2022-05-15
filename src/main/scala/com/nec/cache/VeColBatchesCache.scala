package com.nec.cache

import com.nec.colvector.VeColBatch
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.util.CallContext
import com.nec.vectorengine.VeProcess
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
    import com.nec.util.CallContextOps._
    batches.values.foreach(_.free())
    batches.clear
  }
}
