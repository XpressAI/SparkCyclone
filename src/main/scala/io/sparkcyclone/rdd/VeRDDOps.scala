package io.sparkcyclone.rdd

import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.source
import io.sparkcyclone.spark.transformation.VERewriteStrategy.HashExchangeBuckets
import io.sparkcyclone.data.vector.{VeColBatch, VeColVector}
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.vectorengine.VeProcess
import io.sparkcyclone.data.serialization.DualBatchOrBytes.ColBatchWrapper
import io.sparkcyclone.data.serialization.{DualBatchOrBytes, VeSerializer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.{CoGroupedRDD, RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, TaskContext}

import java.io.{ByteArrayInputStream, DataInputStream}
import scala.reflect.ClassTag

object VeRDDOps extends LazyLogging {
  def exchangeSparkSerialize(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean)(implicit
    context: CallContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (idx, veColBatch) =>
        import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{veProcess, veMetrics}
        logger.debug(s"Preparing to serialize batch ${veColBatch}")
        val r = (idx, veColBatch.toBytes)
        if (cleanUpInput) veColBatch.columns.foreach(_.free())
        logger.debug(s"Completed serializing batch ${veColBatch} (${r._2.length} bytes)")
        r
      }
      .repartitionByKey(serializer = None /* default **/ )
      .map { case (_, ba) =>
        logger.debug(s"Preparing to deserialize batch of size ${ba.length}...")
        import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{veProcess, veMetrics}
        val res = VeColBatch.fromBytes(ba)
        logger.debug(s"Completed deserializing batch ${ba.length} ==> ${res}")
        res
      }

  def exchangeCycloneSerialize(rdd: RDD[(Int, VeColBatch)], cleanUpInput: Boolean, partitions: Int)(
    implicit context: CallContext
  ): RDD[VeColBatch] =
    rdd
      .map { case (idx, veColBatch) =>
        import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
        require(
          veColBatch.nonEmpty,
          s"Expected ${veColBatch} to be non-empty (redundant transfers)"
        )
        if (cleanUpInput) {
          TaskContext.get().addTaskCompletionListener[Unit](_ => veColBatch.free())
        }
        (idx, veColBatch)
      }
      .repartitionByKey(Some(new VeSerializer(rdd.sparkContext.getConf)), partitions)
      .map { case (_, vb) => vb }

  def joinExchange[K: ClassTag](
    left: RDD[(K, VeColBatch)],
    right: RDD[(K, VeColBatch)],
    cleanUpInput: Boolean
  ): RDD[(Iterable[VeColBatch], Iterable[VeColBatch])] = {

    val cg = new CoGroupedRDD(
      Seq(
        left.map { case (num, vcv) =>
          require(vcv.nonEmpty, s"Expected ${vcv} to be non-empty (redundant transfers)")
          import io.sparkcyclone.util.CallContextOps._
          import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
          if (cleanUpInput)
            TaskContext.get().addTaskCompletionListener[Unit](_ => vcv.free())
          (num, ColBatchWrapper(vcv))
        },
        right.map { case (num, vcv) =>
          require(vcv.nonEmpty, s"Expected ${vcv} to be non-empty (redundant transfers)")
          import io.sparkcyclone.util.CallContextOps._
          import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
          if (cleanUpInput)
            TaskContext.get().addTaskCompletionListener[Unit](_ => vcv.free())
          (num, ColBatchWrapper(vcv))
        }
      ),
      new HashPartitioner(HashExchangeBuckets)
    )
    cg.setSerializer(new VeSerializer(left.sparkContext.getConf))
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[DualBatchOrBytes]], w1s.asInstanceOf[Iterable[DualBatchOrBytes]])
    }.map{ case (_, (leftIter, rightIter)) =>
      import io.sparkcyclone.util.CallContextOps._
      import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._

      val leftBatches = leftIter.map(left => left.fold(
        bytesOnly =>
          VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytesOnly.bytes))),
        identity
      ))

      val rightBatches = rightIter.map(right => right.fold(
        bytesOnly =>
          VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytesOnly.bytes))),
        identity
      ))

      (leftBatches, rightBatches)
    }
  }

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs(cleanUpInput: Boolean)(implicit
      veProcess: VeProcess,
      context: CallContext
    ): RDD[VeColVector] =
      rdd
        .map { case (i, vcv) => i -> VeColBatch(List(vcv)) }
        .exchangeBetweenVEs(cleanUpInput)
        .flatMap(_.columns)
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
      cleanUpInput: Boolean = true,
      partitions: Int = rdd.partitions.length
    )(implicit context: CallContext): RDD[VeColBatch] =
      if (UseFastSerializer)
        exchangeCycloneSerialize(rdd, cleanUpInput, partitions = partitions)
      else exchangeSparkSerialize(rdd, cleanUpInput)

    // for single-machine case!
    // def exchangeBetweenVEsNoSer()(implicit veProcess: VeProcess): RDD[VeColBatch] =
    // exchangeLS(rdd)
  }

  implicit class RichRDD(rdd: RDD[VeColBatch]){
    def exchangeBetweenVEs(
      cleanUpInput: Boolean = true,
      partitions: Int = 8
    )(implicit context: CallContext): RDD[VeColBatch] =
      rdd.mapPartitionsWithIndex { (k, b) =>
        import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._

        val (nonEmpty, empty) = b.partition(_.nonEmpty)
        empty.foreach(_.free())

        val batches = nonEmpty.toList
        val partitionRowCount = batches.map(x => x.numRows).sum
        // TODO: Change to something configurable
        val key_fn = if(partitionRowCount < 8000){
          _: Int => 0
        }else{
          i: Int => k + i
        }

        batches.zipWithIndex.map{case (b,i) => (key_fn(i), b)}.iterator
        // TODO: Change partitions to something configurable
      }.exchangeBetweenVEs(cleanUpInput = cleanUpInput, partitions = partitions)
  }
}
