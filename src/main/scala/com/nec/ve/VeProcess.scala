package com.nec.ve

import com.nec.arrow.VeArrowNativeInterface.requireOk
import org.bytedeco.javacpp.LongPointer
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
  def put(from: Long, to: Long, size: Long): Unit
  def get(from: Long, to: ByteBuffer, size: Long): Unit
  def free(memoryLocation: Long): Unit
}

object VeProcess {
  final case class WrappingVeo(veo_proc_handle: veo_proc_handle) extends VeProcess {
    override def allocate(size: Long): Long = ???

    override def putBuffer(byteBuffer: ByteBuffer): Long = {
      val veInputPointer = new LongPointer(8)

      /** No idea why Arrow in some cases returns a ByteBuffer with 0-capacity, so we have to pass a length explicitly! */
      val size = byteBuffer.capacity().toLong
      requireOk(veo.veo_alloc_mem(veo_proc_handle, veInputPointer, size))
      requireOk(
        veo.veo_write_mem(
          veo_proc_handle,
          /** after allocating, this pointer now contains a value of the VE storage address * */
          veInputPointer.get(),
          new org.bytedeco.javacpp.Pointer(byteBuffer),
          size
        )
      )
      veInputPointer.get()
    }

    override def put(from: Long, to: Long, size: Long): Unit = ???

    override def get(from: Long, to: ByteBuffer, size: Long): Unit =
      veo.veo_read_mem(veo_proc_handle, new org.bytedeco.javacpp.Pointer(to), from, size)

    override def free(memoryLocation: Long): Unit =
      veo.veo_free_mem(veo_proc_handle, memoryLocation)
  }
}
