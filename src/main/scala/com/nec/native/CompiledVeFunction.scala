package com.nec.native

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColVector

import java.nio.file.Paths
import scala.reflect.ClassTag

case class CompiledVeFunction(func: CFunction2, outputs: List[CVector]) {
  val libraryPath: String = SparkCycloneDriverPlugin
    .currentCompiler
    .forCode(func.toCodeLinesWithHeaders)
    .toAbsolutePath
    .toString

  def evalFunction(cols: List[VeColVector])(implicit ctx: OriginalCallingContext): VeColBatch = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    println(s"[evalFunction] ${func.name}")

    VeColBatch.fromList(veProcess.execute(libRef, func.name, cols, outputs))
  }

  def evalFunctionOnBatch(batches: Iterator[VeColBatch])(implicit ctx: OriginalCallingContext): Iterator[VeColBatch] = {
    batches.map { batch =>
      evalFunction(batch.cols)
    }
  }

  def evalGrouping[K: ClassTag](
    batchOfBatches: VeColBatch.VeBatchOfBatches
  )(implicit ctx: OriginalCallingContext): Seq[(K, List[VeColVector])] = {
    val libRef = veProcess.loadLibrary(Paths.get(libraryPath))
    println(s"[evalGrouping] ${func.name}")
    veProcess.executeGrouping[K](libRef, func.name, batchOfBatches, outputs)
  }
}