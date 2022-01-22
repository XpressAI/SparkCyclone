package com.nec.ve.colvector

import com.nec.arrow.colvector.{ByteBufferColVector, GenericColVector}
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.SharedVectorEngineMemory.SharedColVector.SharedLocation
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.typesafe.scalalogging.LazyLogging
import io.mappedbus.MemoryMappedFile
import sun.nio.ch.DirectBuffer
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerSHMReadTime, registerSHMWriteTime, registerSHMReadCount, registerSHMWriteCount}

import java.nio.ByteBuffer

final class SharedVectorEngineMemory(mappedFile: MemoryMappedFile, myOffset: Long)
  extends LazyLogging {
  var availableOffset: Long = myOffset

  logger.error(s"Launching mapped file with my offset ${myOffset}")

  def copy(from: DirectBuffer, bytes: Long): Long = {
    copy(from.address(), bytes)
  }
  def copy(from: Long, bytes: Long): Long = {
    val tgt: Long = availableOffset
    measureRunningTime {
      MemoryMappedFile.unsafe.copyMemory(from, mappedFile.addr + tgt, bytes)
      logger.trace(s"Copy: From ${from}, ${bytes} bytes, tgt is ${tgt}")
      availableOffset += bytes
      tgt
    }(registerSHMWriteTime)
  }
  def read(from: Long, bytes: Long): ByteBuffer = {
    logger.trace(s"Reading ${bytes} bytes from ${from}")
    measureRunningTime {
      val byteBuffer = ByteBuffer.allocateDirect(bytes.toInt)
      MemoryMappedFile.unsafe.copyMemory(
        mappedFile.addr + from,
        byteBuffer.asInstanceOf[DirectBuffer].address(),
        bytes
      )
      byteBuffer
    }(registerSHMReadTime)
  }
  def close(): Unit = ()
}

object SharedVectorEngineMemory {

  val Terabyte: Long = Math.pow(1024, 4).toLong

  val ExpectedNumExecutors: Int = 16

  def makeDefault(myOffset: Long): SharedVectorEngineMemory =
    make(path = "/dev/shm/x", myOffset = myOffset, size = ExpectedNumExecutors * Terabyte)

  def make(path: String, myOffset: Long, size: Long): SharedVectorEngineMemory =
    new SharedVectorEngineMemory(mappedFile = new MemoryMappedFile(path, size), myOffset = myOffset)

  object SharedColVector {
    final case class SharedLocation(location: Long, size: Long)
    def fromVeColVector(veColVector: VeColVector)(implicit
      veProcess: VeProcess,
      sharedVectorEngine: SharedVectorEngineMemory,
      originalCallingContext: OriginalCallingContext
    ): SharedColVector = {
      val byteBufferVector = veColVector.toByteBufferVector()
      SharedColVector(
        veColVector.underlying.copy(
          container = None,
          buffers =
            byteBufferVector.underlying.buffers.zip(byteBufferVector.underlying.bufferSizes).map {
              case (Some(byteBuffer: DirectBuffer), size) => {
                registerSHMWriteCount(size)
                Option(SharedLocation(sharedVectorEngine.copy(byteBuffer, size), size))
              }
              case _ => None
            }
        )
      )
    }
  }

  final case class SharedColVector(underlying: GenericColVector[Option[SharedLocation]]) {
    def toVeColVector()(implicit
      veProcess: VeProcess,
      sharedVectorEngine: SharedVectorEngineMemory,
      originalCallingContext: OriginalCallingContext,
      source: VeColVectorSource
    ): VeColVector = {
      ByteBufferColVector(
        underlying.map(
          _.map(sharedLocation =>{
            registerSHMReadCount(underlying.numItems)
            sharedVectorEngine.read(sharedLocation.location, sharedLocation.size)
          })
        )
      ).toVeColVector()
    }
  }

}
