package com.nec.ve

import com.nec.ve.VeColBatch.VeColVector
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

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
      .sortByKey()
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
            logInfo(s"Preparing to serialize batch ${v}")
            val r = (p, (v, v.map(_.serialize())))
            logInfo(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
            r
          },
        preservesPartitioning = true
      )
      .sortByKey()
      .mapPartitions(
        f = iter =>
          iter.map { case (_, (v, ba)) =>
            v.zip(ba).map { case (vv, bb) =>
              logInfo(s"Preparing to deserialize batch ${vv}")
              val res = vv.deserialize(bb)
              logInfo(s"Completed deserializing batch ${vv} --> ${res}")
              res
            }
          },
        preservesPartitioning = true
      )

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs()(implicit veProcess: VeProcess): RDD[VeColVector] = exchange(rdd)
  }
  implicit class RichKeyedRDDL(rdd: RDD[(Int, List[VeColVector])]) {
    def exchangeBetweenVEs()(implicit veProcess: VeProcess): RDD[List[VeColVector]] = exchangeL(rdd)
  }
}
