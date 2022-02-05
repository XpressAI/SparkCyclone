package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeSerializer.VeSerializedContainer
import com.nec.ve.VeSerializer.VeSerializedContainer.{
  VeColBatchHolder,
  VeColBatchesDeserialized,
  VeColBatchesToSerialize
}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.{CoGroupedRDD, RDD, ShuffledRDD}
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
          (p, VeColBatchesToSerialize(List(v)): VeColBatchHolder)
        } finally {
          if (cleanUpInput) {
            TaskContext.get().addTaskCompletionListener[Unit](_ => v.free())
          }
        }
      }
      .repartitionByKey(Some(new VeSerializer(rdd.sparkContext.getConf, cleanUpInput)))
      .map { case (_, vb) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        vb match {
          case VeSerializedContainer.VeColBatchesDeserialized(veColBatch :: Nil) =>
            veColBatch
          case other =>
            sys.error(
              s"Unexpected situation where we received the total data back, and only 1 item; got ${other}"
            )
        }
      }

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

  def joinExchange(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)],
    cleanUpInput: Boolean
  )(implicit originalCallingContext: OriginalCallingContext): RDD[(VeColBatch, VeColBatch)] = {
    val leftPts: RDD[(Int, VeColBatchHolder)] = left
      .map { case (p, v) =>
        if (cleanUpInput) {
          import com.nec.spark.SparkCycloneExecutorPlugin._
          TaskContext.get().addTaskCompletionListener[Unit](_ => v.free())
        }
        (p, VeColBatchesToSerialize(List(v)): VeColBatchHolder)
      }
    val rightPts: RDD[(Int, VeColBatchHolder)] = right
      .map { case (p, v) =>
        if (cleanUpInput) {
          import com.nec.spark.SparkCycloneExecutorPlugin._
          TaskContext.get().addTaskCompletionListener[Unit](_ => v.free())
        }
        (p, VeColBatchesToSerialize(List(v)): VeColBatchHolder)
      }

    val cg = new CoGroupedRDD(Seq(leftPts, rightPts), defaultPartitioner(leftPts, rightPts))
    cg.setSerializer(new VeSerializer(left.sparkContext.getConf, cleanUpInput = false))
    cg.mapValues { case Array(vs, w1s) =>
      (
        vs.asInstanceOf[Iterable[VeColBatchesDeserialized]],
        w1s.asInstanceOf[Iterable[VeColBatchesDeserialized]]
      )
    }.flatMapValues(pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator)
        yield (v.veColBatch.head: VeColBatch, w.veColBatch.head: VeColBatch)
    ).map { case (k, v) => v }
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
