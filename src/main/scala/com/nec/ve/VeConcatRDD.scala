package com.nec.ve

import com.nec.native.{CompiledVeFunction, FunctionTyping}
import com.nec.spark.agile.merge.MergeFunction
import com.nec.colvector.{VeColVector, VeColBatch}
import com.nec.colvector.VeBatchOfBatches
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
        val batchOfBatches = VeBatchOfBatches(batchesList)
        val res = func.evalMultiInFunction(batchOfBatches)
        Iterator(VeColBatch(res))
      }
    }
  }
}

object VeConcatRDD {
  def apply[U: TypeTag, T: TypeTag](rdd: RDD[VeColBatch], funcTypes: FunctionTyping[_, _]): VeConcatRDD[U, VeColBatch] = {
    import com.nec.native.SyntaxTreeOps._

    val outputTypes = funcTypes.input.tpe.toVeTypes

    val funcName = s"merge_${outputTypes.mkString("_")}_1"
    val code = MergeFunction(funcName, outputTypes)
    val func = CompiledVeFunction(
      code.toCFunction,
      code.toVeFunction.namedResults,
      funcTypes
    )

    new VeConcatRDD[U, VeColBatch](new RawVeRDD[T](rdd), func)
  }
}
