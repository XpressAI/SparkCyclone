package com.nec.native

import com.nec.colvector.{VeColBatch, VeColVector, VeBatchOfBatches}
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.{vectorEngine, veProcess, source}

import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.util.CallContext

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

  def evalFunction(batch: VeColBatch)(implicit ctx: CallContext): VeColBatch = {
    import com.nec.spark.SparkCycloneExecutorPlugin.{vectorEngine, source}

    val libRef = veProcess.load(Paths.get(libraryPath))
    val res = VeColBatch(vectorEngine.execute(libRef, func.name, batch.columns.toList, outputs))
    batch.free()
    res
  }

  def evalFunctionOnBatch(batches: Iterator[VeColBatch])(implicit ctx: CallContext): Iterator[VeColBatch] = {
    batches.map { batch =>
      evalFunction(batch)
    }
  }

  def evalGrouping[K: ClassTag](
    batchOfBatches: VeBatchOfBatches
  )(implicit ctx: CallContext): Seq[(K, Seq[VeColVector])] = {
    val libRef = veProcess.load(Paths.get(libraryPath))
    val res = vectorEngine.executeGrouping[K](libRef, func.name, batchOfBatches, outputs)
    batchOfBatches.batches.foreach(_.free())

    res
  }

  def evalMultiInFunction(
    batchOfBatches: VeBatchOfBatches
  )(implicit ctx: CallContext): Seq[VeColVector] = {
    val libRef = veProcess.load(Paths.get(libraryPath))
    val res = vectorEngine.executeMultiIn(libRef, func.name, batchOfBatches, outputs)
    batchOfBatches.batches.foreach(_.free())

    res
  }

  def evalJoinFunction(
    leftBatchOfBatches: VeBatchOfBatches,
    rightBatchOfBatches: VeBatchOfBatches
  )(implicit ctx: CallContext): Seq[VeColVector] = {
    val libRef = veProcess.load(Paths.get(libraryPath))
    val res = vectorEngine.executeJoin(libRef, func.name, leftBatchOfBatches, rightBatchOfBatches, outputs)

    leftBatchOfBatches.batches.foreach(_.free())
    rightBatchOfBatches.batches.foreach(_.free())

    res
  }
}
