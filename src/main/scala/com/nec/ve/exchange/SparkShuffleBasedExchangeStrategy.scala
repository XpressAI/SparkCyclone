package com.nec.ve.exchange
import com.nec.ve.VeProcess
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD

private case object SparkShuffleBasedExchangeStrategy
  extends VeExchangeStrategy
  with Serializable
  with LazyLogging {
  override def exchange(rdd: RDD[(Int, List[VeColVector])], cleanUpInput: Boolean)(implicit
    originalCallingContext: VeProcess.OriginalCallingContext
  ): RDD[List[VeColVector]] =
    rdd
      .map { case (p, v) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        logger.debug(s"Preparing to serialize batch ${v}")
        val r = (p, (v.map(_.underlying.toUnit), v.map(_.serialize())))
        if (cleanUpInput) v.foreach(_.free())
        logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
        r
      }
      .map { case (_, (v, ba)) =>
        v.zip(ba).map { case (vv, bb) =>
          logger.debug(s"Preparing to deserialize batch ${vv}")
          import com.nec.spark.SparkCycloneExecutorPlugin._
          val res = vv.deserialize(bb)
          logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
          res
        }
      }
}
