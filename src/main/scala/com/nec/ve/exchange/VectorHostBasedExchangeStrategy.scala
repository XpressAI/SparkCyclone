package com.nec.ve.exchange

import com.nec.ve.VeProcess
import com.nec.ve.colvector.SharedVectorEngineMemory
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.nec.ve.exchange.VeExchangeStrategy.IntKeyedRDD
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD

private case object VectorHostBasedExchangeStrategy
  extends VeExchangeStrategy
  with LazyLogging
  with Serializable {
  override def exchange(rdd: RDD[(Int, List[VeColVector])], cleanUpInput: Boolean)(implicit
    originalCallingContext: VeProcess.OriginalCallingContext
  ): RDD[List[VeColVector]] =
    rdd
      .map { case (p, vectors) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        logger.debug(s"Preparing to serialize batch ${vectors}...")
        val res = vectors
          .map { vector =>
            try SharedVectorEngineMemory.SharedColVector.fromVeColVector(vector)
            finally if (cleanUpInput) vector.free() else ()
          }
          .zip(vectors.map(_.underlying.toUnit))
        logger.debug(
          s"Completed serializing batch ${vectors} (${res.flatMap(_._1.underlying.bufferSizes).sum} bytes)"
        )
        p -> res
      }
      .repartitionByKey()
      .map { case (_, lvs) =>
        logger.debug(s"Preparing to deserialize batch ${lvs.map(_._1)}")
        val res = lvs.map { case (shr, unit) =>
          import com.nec.spark.SparkCycloneExecutorPlugin._
          shr.toVeColVector()
        }
        logger.debug(s"Completed deserializing batch ${lvs.map(_._1)} --> ${res}")
        res
      }
}
