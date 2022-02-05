package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeSerializer.VeSerializedContainer.VeColBatchToSerialize
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

object VeRDD extends LazyLogging {
  private def exchangeFast(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (p, v) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        try {
          (p, VeColBatchToSerialize(v))
        } finally v.free()
      }
      .repartitionByKey(Some(new VeSerializer(rdd.sparkContext.getConf, cleanUpInput)))
      .map { case (_, vb) => vb.veColBatch }

  private def exchangeSafe(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (p, v) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        try {
          (p, v.serializeToBytes())
        } finally {
          if (cleanUpInput) v.free()
        }
      }
      .repartitionByKey(None)
      .map { case (_, vb) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._

        VeColBatch.readFromBytes(vb)
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

  private implicit class IntKeyedRDD[V: ClassTag](rdd: RDD[(Int, V)]) {
    def repartitionByKey(ser: Option[Serializer]): RDD[(Int, V)] = {
      val srdd = new ShuffledRDD[Int, V, V](rdd, new HashPartitioner(rdd.partitions.length))
      ser.foreach(srdd.setSerializer)
      srdd
    }
  }

  private val UseSafe = false
  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColBatch)]) {
    def exchangeBetweenVEs(
      cleanUpInput: Boolean
    )(implicit originalCallingContext: OriginalCallingContext): RDD[VeColBatch] =
      if (UseSafe) exchangeSafe(rdd, cleanUpInput)
      else exchangeFast(rdd, cleanUpInput)
  }
}
