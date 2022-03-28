package com.nec.ve

import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.colvector.VeColBatch.VeBatchOfBatches
import org.apache.spark.rdd.RDD

import java.nio.file.Paths
import scala.reflect.ClassTag

class VeGroupByRDD[K: ClassTag, T](
  verdd: VeRDD[T],
  func: CFunction2,
  soPath: String,
  outputs: List[CVector]) extends ChainedVeRDD[(K, Iterable[T])](verdd, func, soPath, outputs) {
  override def computeVe(): RDD[_] = {
    verdd.inputs.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val libRef = veProcess.loadLibrary(Paths.get(soPath))

      //val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      val batchOfBatches = VeBatchOfBatches.fromVeColBatches(batches.toList)
      val results = veProcess.executeGrouping[K](libRef, func.name, batchOfBatches, outputs)

      Iterator(VeColBatch.fromList(results))
    }
  }
}

