package com.nec.ve

import com.nec.ve.VeColBatch.VeColVector
import org.apache.spark.rdd.RDD

object VeRDD {
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
  ): RDD[List[VeColVector]] =
    rdd
      .mapPartitions(
        f = iter =>
          iter.map { case (p, v) =>
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            (p, (v, v.map(_.serialize())))
          },
        preservesPartitioning = true
      )
      .sortByKey()
      .mapPartitions(
        f = iter => {
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          iter.map { case (_, (v, ba)) => v.zip(ba).map { case (vv, bb) => vv.deserialize(bb) } }
        },
        preservesPartitioning = true
      )

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs()(implicit veProcess: VeProcess): RDD[VeColVector] = exchange(rdd)
  }
  implicit class RichKeyedRDDL(rdd: RDD[(Int, List[VeColVector])]) {
    def exchangeBetweenVEs(): RDD[List[VeColVector]] = exchangeL(rdd)
  }
}
