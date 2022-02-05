package com.nec.ve

import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.ve.VeColBatch.{VeBatchOfBatches, VeColVector}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import org.apache.spark.metrics.source.ProcessExecutorMetrics
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.veoffload.veo_proc_handle

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.file.Path

trait VeProcess {
  final def readAsBuffer(containerLocation: Long, containerSize: Int): ByteBuffer = {
    val bb = (new BytePointer(containerSize)).asBuffer
    get(containerLocation, bb, containerSize)
    bb
  }

  def validateVectors(list: List[VeColVector]): Unit

  def loadLibrary(path: Path): LibraryReference
  def allocate(size: Long)(implicit context: OriginalCallingContext): Long
  def putBuffer(byteBuffer: ByteBuffer)(implicit context: OriginalCallingContext): Long
  def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit
  def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
    context: OriginalCallingContext
  ): Long
  def get(from: Long, to: ByteBuffer, size: Long): Unit
  def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit

  /** Return a single dataset */
  def execute(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector]

  /** Return multiple datasets - eg for sorting/exchanges */
  def executeMulti(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[(Int, List[VeColVector])]

  def executeMultiIn(
    libraryReference: LibraryReference,
    functionName: String,
    batches: VeBatchOfBatches,
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector]

}

object VeProcess {
  def wrappingVeo(
    _veo_proc: veo_proc_handle,
    source: VeColBatch.VeColVectorSource,
    metrics: VeProcessMetrics
  ): VeProcess = VeProcessVeo(_veo_proc, source, metrics)

  def deferredVeProcess(f: () => VeProcess): VeProcess = VeProcessDeferred(f)

  final case class OriginalCallingContext(fullName: sourcecode.FullName, line: sourcecode.Line) {
    def renderString: String = s"${fullName.value}#${line.value}"
  }

  object OriginalCallingContext {
    def make(implicit
      fullName: sourcecode.FullName,
      line: sourcecode.Line
    ): OriginalCallingContext =
      OriginalCallingContext(fullName, line)

    object Automatic {
      implicit def originalCallingContext(implicit
        fullName: sourcecode.FullName,
        line: sourcecode.Line
      ): OriginalCallingContext = make
    }
  }

  final case class LibraryReference(value: Long)

}
