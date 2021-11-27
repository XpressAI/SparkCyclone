package com.nec.ve

import org.bytedeco.veoffload.veo_proc_handle

import java.nio.ByteBuffer

trait VeProcess {
  def allocate(size: Long): Long
  def putBuffer(byteBuffer: ByteBuffer): Long
  def put(from: Long, to: Long, size: Long): Unit
  def get(from: Long, to: Long, size: Long): Unit
  def free(memoryLocation: Long): Unit
}

object VeProcess {
  final case class WrappingVeo(veo_proc_handle: veo_proc_handle) extends VeProcess {
    override def allocate(size: Long): Long = ???

    override def putBuffer(byteBuffer: ByteBuffer): Long = ???

    override def put(from: Long, to: Long, size: Long): Unit = ???

    override def get(from: Long, to: Long, size: Long): Unit = ???

    override def free(memoryLocation: Long): Unit = ???
  }
}
