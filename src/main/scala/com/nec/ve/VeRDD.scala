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
      .mapPartitions(f =
        iter =>
          iter.map { case (p, v) =>
            try {
              (p, (v.underlying.toUnit, v.serialize()))
            } finally v.free()
          }
      )
      .repartitionByKey()
      .mapPartitions(f = iter => iter.map { case (_, (v, ba)) => v.deserialize(ba) })

  def exchangeL(rdd: RDD[(Int, List[VeColVector])], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[List[VeColVector]] =
    rdd
      .mapPartitions(f =
        iter =>
          iter.map { case (p, v) =>
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            logger.debug(s"Preparing to serialize batch ${v}")
            val r = (p, (v.map(_.underlying.toUnit), v.map(_.serialize())))
            if (cleanUpInput) v.foreach(_.free())
            logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
            r
          }
      )
      .repartitionByKey()
      .mapPartitions(f =
        iter =>
          iter.map { case (_, (v, ba)) =>
            v.zip(ba).map { case (vv, bb) =>
              logger.debug(s"Preparing to deserialize batch ${vv}")
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              val res = vv.deserialize(bb)
              logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
              res
            }
          }
      )

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
