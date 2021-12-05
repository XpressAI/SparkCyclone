package com.nec.ve

import com.nec.ve.VeColBatch.VeColVector
import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

object VeRDD extends Logging {
  def exchange(rdd: RDD[(Int, VeColVector)])(implicit veProcess: VeProcess): RDD[VeColVector] =
    rdd
      .mapPartitions(
        f = iter =>
          iter.map { case (p, v) =>
            (p, (v, v.serialize()))
          },
        preservesPartitioning = true
      )
      .repartitionByKey()
      .mapPartitions(
        f = iter => iter.map { case (_, (v, ba)) => v.deserialize(ba) },
        preservesPartitioning = true
      )

  def exchangeL(
    rdd: RDD[(Int, List[VeColVector])]
  )(implicit veProcess: VeProcess): RDD[List[VeColVector]] =
    rdd
      .mapPartitions(
        f = iter =>
          iter.map { case (p, v) =>
//            logInfo(s"Preparing to serialize batch ${v}")
            val r = (p, (v, v.map(_.serialize())))
//            logInfo(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
            r
          },
        preservesPartitioning = true
      )
      .repartitionByKey()
      .mapPartitions(
        f = iter =>
          iter.map { case (_, (v, ba)) =>
            v.zip(ba).map { case (vv, bb) =>
//              logInfo(s"Preparing to deserialize batch ${vv}")
              val res = vv.deserialize(bb)
//              logInfo(s"Completed deserializing batch ${vv} --> ${res}")
              res
            }
          },
        preservesPartitioning = true
      )
  def exchangeLS(rdd: RDD[(Int, List[VeColVector])])(implicit
    veProcess: VeProcess
  ): RDD[List[VeColVector]] =
    rdd.repartitionByKey().mapPartitions(f = _.map(_._2), preservesPartitioning = true)

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs()(implicit veProcess: VeProcess): RDD[VeColVector] = exchange(rdd)
  }

  implicit class IntKeyedRDD[V: ClassTag](rdd: RDD[(Int, V)]) {
    def repartitionByKey(): RDD[(Int, V)] =
      new ShuffledRDD[Int, V, V](rdd, new HashPartitioner(rdd.partitions.length))
  }
  implicit class RichKeyedRDDL(rdd: RDD[(Int, List[VeColVector])]) {
    def exchangeBetweenVEs()(implicit veProcess: VeProcess): RDD[List[VeColVector]] = exchangeL(rdd)
    // for single-machine case!
    def exchangeBetweenVEsNoSer()(implicit veProcess: VeProcess): RDD[List[VeColVector]] =
      exchangeLS(rdd)
  }
}
