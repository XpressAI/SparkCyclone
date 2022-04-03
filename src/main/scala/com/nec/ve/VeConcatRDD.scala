package com.nec.ve

import com.nec.native.CompiledVeFunction
import com.nec.ve.colvector.VeColBatch.VeBatchOfBatches
import org.apache.spark.rdd.RDD

import scala.language.experimental.macros
import scala.reflect.runtime.universe.TypeTag

class VeConcatRDD[U: TypeTag, T: TypeTag](
  rdd: VeRDD[T],
  func: CompiledVeFunction,
) extends MappedVeRDD[U, T](rdd, func) {
  override def computeVe(): RDD[VeColBatch] = {
    rdd.inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
      val batchesList = batches.toList
      if (batchesList.isEmpty) {
        Iterator()
      } else {
        val batchOfBatches = VeBatchOfBatches.fromVeColBatches(batchesList)
        val res = func.evalMultiInFunction(batchOfBatches)
        Iterator(VeColBatch.fromList(res))
      }
    }
  }
}
