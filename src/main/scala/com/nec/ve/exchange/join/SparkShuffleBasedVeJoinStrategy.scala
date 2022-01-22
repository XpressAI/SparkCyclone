package com.nec.ve.exchange.join

import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD

private case object SparkShuffleBasedVeJoinStrategy extends VeJoinStrategy with LazyLogging with Serializable {
  override def join(
    left: RDD[(Int, List[VeColVector])],
    right: RDD[(Int, List[VeColVector])],
    cleanUpInput: Boolean
  ): RDD[(List[VeColVector], List[VeColVector])] = {
    {
      val leftPts = left
        .map { case (p, v) =>
          import com.nec.spark.SparkCycloneExecutorPlugin._
          logger.debug(s"Preparing to serialize batch ${v}")
          val r = (p, (v.map(_.underlying.toUnit), v.map(_.serialize())))
          import OriginalCallingContext.Automatic._
          if (cleanUpInput) v.foreach(_.free())
          logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
          r
        }
      val rightPts = right
        .map { case (p, v) =>
          import com.nec.spark.SparkCycloneExecutorPlugin._
          logger.debug(s"Preparing to serialize batch ${v}")
          val r = (p, (v.map(_.underlying.toUnit), v.map(_.serialize())))
          import OriginalCallingContext.Automatic._
          if (cleanUpInput) v.foreach(_.free())
          logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
          r
        }

      leftPts
        .join(rightPts)
        .filter { case (_, ((v1, _), (v2, _))) =>
          v1.nonEmpty && v2.nonEmpty
        }
        .map { case (_, ((v1, ba1), (v2, ba2))) =>
          val first = v1.zip(ba1).map { case (vv, bb) =>
            logger.debug(s"Preparing to deserialize batch ${vv}")
            import OriginalCallingContext.Automatic._
            import com.nec.spark.SparkCycloneExecutorPlugin._
            val res = vv.deserialize(bb)
            logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
            res
          }
          val second = v2.zip(ba2).map { case (vv, bb) =>
            logger.debug(s"Preparing to deserialize batch ${vv}")
            import OriginalCallingContext.Automatic._
            import com.nec.spark.SparkCycloneExecutorPlugin._
            val res = vv.deserialize(bb)
            logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
            res
          }

          (first, second)
        }
    }
  }
}
