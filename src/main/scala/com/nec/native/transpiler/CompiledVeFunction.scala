package com.nec.native.transpiler

import com.nec.colvector.{VeColBatch, VeColVector, VeBatchOfBatches}
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.util.CallContext
import scala.reflect.ClassTag
import java.nio.file.Paths

case class CompiledVeFunction(func: CFunction2, outputs: List[CVector], @transient types: FunctionTyping[_, _]) {
  import com.nec.spark.SparkCycloneExecutorPlugin.{vectorEngine, veProcess, source}

  lazy val libraryPath: String = {
    if (SparkCycloneDriverPlugin.currentCompiler != null) {
      SparkCycloneDriverPlugin
        .currentCompiler
        .build(func.toCodeLinesWithHeaders.cCode)
        .toAbsolutePath
        .toString
    } else {
      "no compiler defined"
    }
  }

  def evalFunction(batch: VeColBatch)(implicit ctx: CallContext): VeColBatch = {
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
