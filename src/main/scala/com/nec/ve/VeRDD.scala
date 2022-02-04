package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

object VeRDD extends LazyLogging {
  def exchange(rdd: RDD[(Int, VeColVector)])(implicit
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext
  ): RDD[VeColVector] =
    rdd
      .map { case (p, v) =>
        try {
          (p, (v.underlying.toUnit, v.serialize()))
        } finally v.free()
      }
      .repartitionByKey()
      .map { case (_, (v, ba)) => v.deserialize(ba) }

  def exchangeL(rdd: RDD[(Int, List[VeColVector])], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[List[VeColVector]] =
    rdd
      .map { case (p, v) =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        logger.debug(s"Preparing to serialize batch ${v}")
        val r = (p, (v.map(_.underlying.toUnit), v.map(_.serialize())))
        if (cleanUpInput) v.foreach(_.free())
        logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
        r
      }
      .repartitionByKey()
      .map { case (_, (v, ba)) =>
        v.zip(ba).map { case (vv, bb) =>
          logger.debug(s"Preparing to deserialize batch ${vv}")
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          val res = vv.deserialize(bb)
          logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
          res
        }
      }

  def joinExchangeLB(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)],
    cleanUpInput: Boolean
  ): RDD[(List[VeColVector], List[VeColVector])] = {
    joinExchangeL(
      left = left.map { case (k, b) => k -> b.cols },
      right = right.map { case (k, b) => k -> b.cols },
      cleanUpInput = cleanUpInput
    )
  }

  def joinExchangeL(
    left: RDD[(Int, List[VeColVector])],
    right: RDD[(Int, List[VeColVector])],
    cleanUpInput: Boolean
  ): RDD[(List[VeColVector], List[VeColVector])] = {
    {
      val leftPts = left
        .map { case (p, v) =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          logger.debug(s"Preparing to serialize batch ${v}")
          val r = (p, (v.map(_.underlying.toUnit), v.map(_.serialize())))
          import OriginalCallingContext.Automatic._
          if (cleanUpInput) v.foreach(_.free())
          logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
          r
        }
      val rightPts = right
        .map { case (p, v) =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
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
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            import OriginalCallingContext.Automatic._
            val res = vv.deserialize(bb)
            logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
            res
          }
          val second = v2.zip(ba2).map { case (vv, bb) =>
            logger.debug(s"Preparing to deserialize batch ${vv}")
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            import OriginalCallingContext.Automatic._
            val res = vv.deserialize(bb)
            logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
            res
          }

          (first, second)
        }
    }
  }

  def exchangeLS(rdd: RDD[(Int, List[VeColVector])])(implicit
    veProcess: VeProcess
  ): RDD[List[VeColVector]] = rdd.repartitionByKey().map(_._2)

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs()(implicit
      veProcess: VeProcess,
      originalCallingContext: OriginalCallingContext
    ): RDD[VeColVector] = exchange(rdd)
  }

  private implicit class IntKeyedRDD[V: ClassTag](rdd: RDD[(Int, V)]) {
    def repartitionByKey(): RDD[(Int, V)] =
      new ShuffledRDD[Int, V, V](rdd, new HashPartitioner(rdd.partitions.length))
  }
  implicit class RichKeyedRDDL(rdd: RDD[(Int, List[VeColVector])]) {
    def exchangeBetweenVEs(cleanUpInput: Boolean)(implicit
      originalCallingContext: OriginalCallingContext
    ): RDD[List[VeColVector]] =
      exchangeL(rdd, cleanUpInput)
    // for single-machine case!
    // def exchangeBetweenVEsNoSer()(implicit veProcess: VeProcess): RDD[List[VeColVector]] =
    // exchangeLS(rdd)
  }
}
