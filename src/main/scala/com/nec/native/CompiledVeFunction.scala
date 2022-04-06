package com.nec.native

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColVector

import java.nio.file.Paths
import scala.reflect.ClassTag

case class CompiledVeFunction(func: CFunction2, outputs: List[CVector], @transient types: FunctionTyping[_, _]) {
  val libraryPath: String = {
    if (SparkCycloneDriverPlugin.currentCompiler != null) {
      SparkCycloneDriverPlugin
        .currentCompiler
        .forCode(func.toCodeLinesWithHeaders)
        .toAbsolutePath
        .toString
    } else {
      "no compiler defined"
    }
  }

  def evalFunction(batch: VeColBatch)(implicit ctx: OriginalCallingContext): VeColBatch = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    VeColBatch.fromList(veProcess.execute(libRef, func.name, batch.cols, outputs))
  }

  def evalFunctionOnBatch(batches: Iterator[VeColBatch])(implicit ctx: OriginalCallingContext): Iterator[VeColBatch] = {
    batches.map { batch =>
      evalFunction(batch)
    }
  }

  def evalGrouping[K: ClassTag](
    batchOfBatches: VeColBatch.VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): Seq[(K, List[VeColVector])] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    veProcess.executeGrouping[K](libRef, func.name, batchOfBatches, outputs)
  }

  def evalMultiInFunction(
    batchOfBatches: VeColBatch.VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): List[VeColVector] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    veProcess.executeMultiIn(libRef, func.name, batchOfBatches, outputs)
  }

  def evalJoinFunction(
    leftBatchOfBatches: VeColBatch.VeBatchOfBatches,
    rightBatchOfBatches: VeColBatch.VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): List[VeColVector] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    veProcess.executeJoin(libRef, func.name, leftBatchOfBatches, rightBatchOfBatches, outputs)
  }
}
