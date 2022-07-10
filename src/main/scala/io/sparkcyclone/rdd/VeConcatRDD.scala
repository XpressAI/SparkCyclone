package io.sparkcyclone.rdd

import io.sparkcyclone.data.vector.{VeBatchOfBatches, VeColVector, VeColBatch}
import io.sparkcyclone.native.transpiler.{CompiledVeFunction, FunctionTyping}
import io.sparkcyclone.spark.codegen.merge.MergeFunction
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.rdd.RDD

class VeConcatRDD[U: TypeTag, T: TypeTag](
  rdd: VeRDD[T],
  func: CompiledVeFunction,
) extends MappedVeRDD[U, T](rdd, func) {
  override def computeVe(): RDD[VeColBatch] = {
    rdd.inputs.mapPartitions { batches =>
     import io.sparkcyclone.util.CallContextOps._
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
    import io.sparkcyclone.native.transpiler.SyntaxTreeOps._

    val outputTypes = funcTypes.input.tpe.toVeTypes

    val funcName = s"merge_${outputTypes.mkString("_")}_1"
    val code = MergeFunction(funcName, outputTypes)
    val func = CompiledVeFunction(
      code.toCFunction,
      code.toVeFunction.outputs,
      funcTypes
    )

    new VeConcatRDD[U, VeColBatch](new RawVeRDD[T](rdd), func)
  }
}
