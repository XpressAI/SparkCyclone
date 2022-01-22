package com.nec.ve.exchange

import com.nec.arrow.WithTestAllocator
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.agile.CFunctionGeneration
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.VERDDSpec.doubleBatches
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.exchange.VeExchangeStrategy.IntKeyedRDD
import org.apache.arrow.vector.Float8Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ExchangeBatches {

  private def exchange(rdd: RDD[(Int, VeColVector)])(implicit
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

  private implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs()(implicit
                             veProcess: VeProcess,
                             originalCallingContext: OriginalCallingContext
    ): RDD[VeColVector] = exchange(rdd)
  }
  val MultiFunctionName = "f_multi"
  import OriginalCallingContext.Automatic._

  def exchangeBatches(sparkSession: SparkSession, pathStr: String): RDD[Double] = {

    import SparkCycloneExecutorPlugin.{veProcess => veProc}

    doubleBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 501, step = 1, numSlices = 4)
        .map(_.toDouble)
    }
      .mapPartitions(
        f = veIterator =>
          veIterator
            .map(arrowVec => {
              import SparkCycloneExecutorPlugin.source
              try VeColVector.fromArrowVector(arrowVec)
              finally arrowVec.close()
            })
            .flatMap(veColVector => {
              import SparkCycloneExecutorPlugin.source
              try {
                val ref = veProc.loadLibrary(java.nio.file.Paths.get(pathStr))

                veProc
                  .executeMulti(
                    ref,
                    MultiFunctionName,
                    List(veColVector),
                    List(CFunctionGeneration.VeScalarType.veNullableDouble.makeCVector("o_dbl"))
                  )
                  .map { case (k, vs) => (k, vs.head) }
              } finally veColVector.free()
            }),
        preservesPartitioning = true
      )
      .exchangeBetweenVEs()
      .mapPartitions(vectorIter =>
        Iterator
          .continually {
            vectorIter.flatMap { vector =>
              WithTestAllocator { implicit alloc =>
                import SparkCycloneExecutorPlugin.source
                try {
                  val vec = vector.toArrowVector().asInstanceOf[Float8Vector]
                  val vl = vec.toList
                  try if (vl.isEmpty) None else Some(vl.max)
                  finally vec.close()
                } finally vector.free()
              }
            }.max
          }
          .take(1)
      )
  }

}
