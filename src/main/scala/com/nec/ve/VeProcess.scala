package com.nec.ve

import com.nec.arrow.VeArrowNativeInterface.requireOk
import com.nec.spark.agile.CFunctionGeneration.VeType
import com.nec.ve.VeColBatch.VeColVector
import org.bytedeco.javacpp.{BytePointer, LongPointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.nio.ByteBuffer

trait VeProcess {
  def readAsBuffer(containerLocation: Long, containerSize: Int): ByteBuffer = {
    val bb = ByteBuffer.allocateDirect(containerSize)
    get(containerLocation, bb, containerSize)
    bb
  }

  def allocate(size: Long): Long
  def putBuffer(byteBuffer: ByteBuffer): Long
  def get(from: Long, to: ByteBuffer, size: Long): Unit
  def free(memoryLocation: Long): Unit
  def execute(
    functionName: String,
    cols: List[VeColVector],
    results: List[VeType]
  ): List[VeColVector]
}

object VeProcess {
  final case class WrappingVeo(veo_proc_handle: veo_proc_handle) extends VeProcess {
    override def allocate(size: Long): Long = {
      val veInputPointer = new LongPointer(8)
      veo.veo_alloc_mem(veo_proc_handle, veInputPointer, size)
      veInputPointer.get()
    }

    override def putBuffer(byteBuffer: ByteBuffer): Long = {
      val memoryLocation = allocate(byteBuffer.capacity().toLong)
      requireOk(
        veo.veo_write_mem(
          veo_proc_handle,
          memoryLocation,
          new org.bytedeco.javacpp.Pointer(byteBuffer),
          byteBuffer.capacity().toLong
        )
      )
      memoryLocation
    }

    override def get(from: Long, to: ByteBuffer, size: Long): Unit =
      veo.veo_read_mem(veo_proc_handle, new org.bytedeco.javacpp.Pointer(to), from, size)

    override def free(memoryLocation: Long): Unit =
      veo.veo_free_mem(veo_proc_handle, memoryLocation)

    override def execute(
      functionName: String,
      cols: List[VeColVector],
      results: List[VeType]
    ): List[VeColVector] = {
      val our_args = veo.veo_args_alloc()
      cols.zipWithIndex.foreach { case (vcv, index) =>
        veo.veo_args_set_stack(
          our_args,
          0,
          index,
          new BytePointer(vcv.containerLocation),
          vcv.containerSize
        )
      }
      val outContainers = results.map { veType =>
        allocate(veType.containerSize)
      }
      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = reIdx + cols.size
        veo.veo_args_set_stack(
          our_args,
          1,
          index,
          new BytePointer(outContainers(reIdx)),
          vet.containerSize
        )
      }

      outContainers.zip(results).map { case (outContainerLocation, r) =>
        val byteBuffer = readAsBuffer(outContainerLocation, r.containerSize)

        VeColVector(
          numItems = byteBuffer.getInt(byteBuffer.getInt(8)),
          veType = r,
          containerLocation = outContainerLocation,
          bufferLocations = List(byteBuffer.getInt(16), byteBuffer.getInt(8))
        )
      }
    }
  }
}
