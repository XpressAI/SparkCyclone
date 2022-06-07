package com.nec.cache

import com.nec.colvector.{VeColBatch, VeColVectorSource}
import com.nec.util.PointerOps.ExtendedPointer
import org.bytedeco.javacpp.{BytePointer, LongPointer}

trait TransferDescriptor {
  def buffer: BytePointer
  def resultBuffer: LongPointer
  def resultToColBatch(implicit source: VeColVectorSource): VeColBatch
  def close: Unit
  def nonEmpty: Boolean

  def print: Unit = {
    println(s"Transfer Buffer = \n${buffer.hexdump}\n")
  }

  def toSeq: Seq[Byte] = {
    val buf = buffer
    val array = Array.ofDim[Byte](buffer.limit().toInt)
    buf.get(array)
    array.toSeq
  }
}
