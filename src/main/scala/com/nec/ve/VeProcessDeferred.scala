package com.nec.ve

import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.colvector.VeColBatch.{VeBatchOfBatches, VeColVector}
import com.typesafe.scalalogging.LazyLogging

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.file.Path

private[ve] final case class VeProcessDeferred(f: () => VeProcess)
  extends VeProcess
  with LazyLogging {

  override def validateVectors(list: List[VeColVector]): Unit = f().validateVectors(list)
  override def loadLibrary(path: Path): LibraryReference = f().loadLibrary(path)

  override def allocate(size: Long)(implicit context: OriginalCallingContext): Long =
    f().allocate(size)

  override def putBuffer(byteBuffer: ByteBuffer)(implicit context: OriginalCallingContext): Long =
    f().putBuffer(byteBuffer)

  override def get(from: Long, to: ByteBuffer, size: Long): Unit = f().get(from, to, size)

  override def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit =
    f().free(memoryLocation)

  override def execute(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector] =
    f().execute(libraryReference, functionName, cols, results)

  /** Return multiple datasets - eg for sorting/exchanges */
  override def executeMulti(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[(Int, List[VeColVector])] =
    f().executeMulti(libraryReference, functionName, cols, results)

  override def executeMultiIn(
    libraryReference: LibraryReference,
    functionName: String,
    batches: VeBatchOfBatches,
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector] =
    f().executeMultiIn(libraryReference, functionName, batches, results)

  override def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit =
    f().writeToStream(outStream, bufPos, bufLen)

  override def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
    context: OriginalCallingContext
  ): Long = f().loadFromStream(inputStream, bytes)
}
