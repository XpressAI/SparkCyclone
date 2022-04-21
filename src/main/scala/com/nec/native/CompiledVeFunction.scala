package com.nec.native

import com.nec.colvector.{VeColBatch, VeColVector, VeBatchOfBatches}
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.VeProcess.OriginalCallingContext

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
    import com.nec.spark.SparkCycloneExecutorPlugin.source

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    val res = VeColBatch(veProcess.execute(libRef, func.name, batch.columns.toList, outputs))
    batch.free()
    res
  }

  def evalFunctionOnBatch(batches: Iterator[VeColBatch])(implicit ctx: OriginalCallingContext): Iterator[VeColBatch] = {
    batches.map { batch =>
      evalFunction(batch)
    }
  }

  def evalGrouping[K: ClassTag](
    batchOfBatches: VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): Seq[(K, List[VeColVector])] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    import com.nec.spark.SparkCycloneExecutorPlugin.source

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    val res = veProcess.executeGrouping[K](libRef, func.name, batchOfBatches, outputs)
    batchOfBatches.batches.foreach(_.free())

    res
  }

  def evalMultiInFunction(
    batchOfBatches: VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): List[VeColVector] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    import com.nec.spark.SparkCycloneExecutorPlugin.source

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    val res = veProcess.executeMultiIn(libRef, func.name, batchOfBatches, outputs)
    batchOfBatches.batches.foreach(_.free())

    res
  }

  def evalJoinFunction(
    leftBatchOfBatches: VeBatchOfBatches,
    rightBatchOfBatches: VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): List[VeColVector] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    import com.nec.spark.SparkCycloneExecutorPlugin.source

    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    val res = veProcess.executeJoin(libRef, func.name, leftBatchOfBatches, rightBatchOfBatches, outputs)

    leftBatchOfBatches.batches.foreach(_.free())
    rightBatchOfBatches.batches.foreach(_.free())

    res
  }
}
