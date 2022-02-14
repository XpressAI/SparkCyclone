package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.serializer.VeSerializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

object VeRDD extends LazyLogging {
  def exchangeSparkSerialize(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (p, v) =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        logger.debug(s"Preparing to serialize batch ${v}")
        val r = (p, v.serialize())
        if (cleanUpInput) v.cols.foreach(_.free())
        logger.debug(s"Completed serializing batch ${v} (${r._2.length} bytes)")
        r
      }
      .repartitionByKey(serializer = None /* default **/ )
      .map { case (_, ba) =>
        logger.debug(s"Preparing to deserialize batch of size ${ba.length}...")
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        val res = VeColBatch.deserialize(ba)
        logger.debug(s"Completed deserializing batch ${ba.length} ==> ${res}")
        res
      }

  def exchangeCycloneSerialize(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean, partitions: Int)(
    implicit originalCallingContext: OriginalCallingContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (p, v) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._

        if (cleanUpInput) {
          TaskContext.get().addTaskCompletionListener[Unit](_ => v.free())
        }
        (p, v)
      }
      .repartitionByKey(Some(new VeSerializer(rdd.sparkContext.getConf, cleanUpInput)), partitions)
      .map { case (_, vb) => vb }

  def joinExchangeLB(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)],
    cleanUpInput: Boolean
  ): RDD[(VeColBatch, VeColBatch)] = {
    joinExchangeL(left = left, right = right, cleanUpInput = cleanUpInput)
  }

  def joinExchangeL(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)],
    cleanUpInput: Boolean
  ): RDD[(VeColBatch, VeColBatch)] = {
    {
      val leftPts = left
        .map { case (p, v) =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          logger.debug(s"Preparing to serialize batch ${v}")
          val r = (p, (v.cols.map(_.underlying.toUnit), v.cols.map(_.serialize())))
          import OriginalCallingContext.Automatic._
          if (cleanUpInput) v.cols.foreach(_.free())
          logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
          r
        }
      val rightPts = right
        .map { case (p, v) =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          logger.debug(s"Preparing to serialize batch ${v}")
          val r = (p, (v.cols.map(_.underlying.toUnit), v.cols.map(_.serialize())))
          import OriginalCallingContext.Automatic._
          if (cleanUpInput) v.cols.foreach(_.free())
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

          (VeColBatch.fromList(first), VeColBatch.fromList(second))
        }
    }
  }

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs(cleanUpInput: Boolean)(implicit
      veProcess: VeProcess,
      originalCallingContext: OriginalCallingContext
    ): RDD[VeColVector] =
      rdd
        .map { case (i, vcv) => i -> VeColBatch.fromList(List(vcv)) }
        .exchangeBetweenVEs(cleanUpInput)
        .flatMap(_.cols)
  }

  private implicit class IntKeyedRDD[V: ClassTag](rdd: RDD[(Int, V)]) {
    def repartitionByKey(
      serializer: Option[Serializer],
      partitions: Int = rdd.partitions.length
    ): RDD[(Int, V)] = {
      val out = new ShuffledRDD[Int, V, V](rdd, new HashPartitioner(partitions))
      serializer.foreach(out.setSerializer)
      out
    }
  }

  val UseFastSerializer = true

  implicit class RichKeyedRDDL(rdd: RDD[(Int, VeColBatch)]) {
    def exchangeBetweenVEs(
      cleanUpInput: Boolean
    )(implicit originalCallingContext: OriginalCallingContext): RDD[VeColBatch] =
      if (UseFastSerializer)
        exchangeCycloneSerialize(rdd, cleanUpInput, partitions = rdd.partitions.length)
      else exchangeSparkSerialize(rdd, cleanUpInput)

    // for single-machine case!
    // def exchangeBetweenVEsNoSer()(implicit veProcess: VeProcess): RDD[VeColBatch] =
    // exchangeLS(rdd)
  }
}
