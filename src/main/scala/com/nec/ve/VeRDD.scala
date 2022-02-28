package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.VERewriteStrategy.HashExchangeBuckets
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.serializer.DualBatchOrBytes.{BytesOnly, ColBatchWrapper}
import com.nec.ve.serializer.{DualBatchOrBytes, VeSerializer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.{CoGroupedRDD, RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer

import java.io.{ByteArrayInputStream, DataInputStream}
import scala.reflect.ClassTag

object VeRDD extends LazyLogging {
  def exchangeSparkSerialize(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (idx, veColBatch) =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics._
        logger.debug(s"Preparing to serialize batch ${veColBatch}")
        val r = (idx, veColBatch.serialize())
        if (cleanUpInput) veColBatch.cols.foreach(_.free())
        logger.debug(s"Completed serializing batch ${veColBatch} (${r._2.length} bytes)")
        r
      }
      .repartitionByKey(serializer = None /* default **/ )
      .map { case (_, ba) =>
        logger.debug(s"Preparing to deserialize batch of size ${ba.length}...")
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics._
        val res = VeColBatch.deserialize(ba)
        logger.debug(s"Completed deserializing batch ${ba.length} ==> ${res}")
        res
      }

  def exchangeCycloneSerialize(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean, partitions: Int)(
    implicit originalCallingContext: OriginalCallingContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (idx, veColBatch) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        require(
          veColBatch.nonEmpty,
          s"Expected ${veColBatch} to be non-empty (redundant transfers)"
        )
        if (cleanUpInput) {
          TaskContext.get().addTaskCompletionListener[Unit](_ => veColBatch.free())
        }
        (idx, veColBatch)
      }
      .repartitionByKey(Some(new VeSerializer(rdd.sparkContext.getConf, cleanUpInput)), partitions)
      .map { case (_, vb) => vb }

  def joinExchange(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)],
    cleanUpInput: Boolean
  ): RDD[(VeColBatch, VeColBatch)] = {

    val cg = new CoGroupedRDD(
      Seq(
        left.map { case (num, vcv) =>
          require(vcv.nonEmpty, s"Expected ${vcv} to be non-empty (redundant transfers)")
          import com.nec.spark.SparkCycloneExecutorPlugin._
          import OriginalCallingContext.Automatic._
          if (cleanUpInput)
            TaskContext.get().addTaskCompletionListener[Unit](_ => vcv.free())
          (num, ColBatchWrapper(vcv))
        },
        right.map { case (num, vcv) =>
          require(vcv.nonEmpty, s"Expected ${vcv} to be non-empty (redundant transfers)")
          import com.nec.spark.SparkCycloneExecutorPlugin._
          import OriginalCallingContext.Automatic._
          if (cleanUpInput)
            TaskContext.get().addTaskCompletionListener[Unit](_ => vcv.free())
          (num, ColBatchWrapper(vcv))
        }
      ),
      new HashPartitioner(HashExchangeBuckets)
    )
    cg.setSerializer(new VeSerializer(left.sparkContext.getConf, cleanUpInput))
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[DualBatchOrBytes]], w1s.asInstanceOf[Iterable[DualBatchOrBytes]])
    }.flatMapValues(pair =>
      for {
        v <- pair._1.iterator
        w <- pair._2.iterator
      } yield {
        import com.nec.spark.SparkCycloneExecutorPlugin._
        import OriginalCallingContext.Automatic._
        (
          v.fold(
            bytesOnly =>
              VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytesOnly.bytes))),
            identity
          ),
          w.fold(
            bytesOnly =>
              VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytesOnly.bytes))),
            identity
          )
        )
      }
    ).map { case (_, (a, b)) => (a, b) }
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
