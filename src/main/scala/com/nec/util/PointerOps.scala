package com.nec.util

import scala.reflect.ClassTag
import org.bytedeco.javacpp._

object PointerOps {
  implicit class ExtendedPointer(buffer: Pointer) {
    def nbytes: Long = {
      buffer.limit * buffer.sizeof
    }
  }

  implicit class ExtendedBytePointer(buffer: BytePointer) {
    def toArray: Array[Byte] = {
      val array = Array.ofDim[Byte](buffer.limit().toInt)
      buffer.get(array)
      array
    }

    def slice(offset: Long, size: Long): BytePointer = {
      require(offset > 0, "offset must be > 0")
      require(size >= 0, "size must be >= 0")

      val outbuffer = new BytePointer(size)
      Pointer.memcpy(outbuffer, buffer.position(offset), size)
      buffer.position(0)
      outbuffer
    }

    def toHex: Array[String] = {
      toArray.map { b => String.format("%02x", Byte.box(b)) }
    }

    def hexdump: String = {
      toHex.sliding(16, 16)
        .map { chunk => chunk.mkString(" ") }
        .mkString("\n")
    }

    def as[T <: Pointer : ClassTag]: T = {
      val klass = implicitly[ClassTag[T]].runtimeClass
      val cbuffer = klass.getConstructor(classOf[Pointer]).newInstance(buffer).asInstanceOf[T]
      cbuffer.limit(buffer.limit() / cbuffer.sizeof)
    }
  }

  implicit class ExtendedShortPointer(buffer: ShortPointer) {
    def toArray: Array[Short] = {
      val array = Array.ofDim[Short](buffer.limit().toInt)
      buffer.get(array)
      array
    }
  }

  implicit class ExtendedIntPointer(buffer: IntPointer) {
    def toArray: Array[Int] = {
      val array = Array.ofDim[Int](buffer.limit().toInt)
      buffer.get(array)
      array
    }
  }

  implicit class ExtendedLongPointer(buffer: LongPointer) {
    def toArray: Array[Long] = {
      val array = Array.ofDim[Long](buffer.limit().toInt)
      buffer.get(array)
      array
    }
  }

  implicit class ExtendedFloatPointer(buffer: FloatPointer) {
    def toArray: Array[Float] = {
      val array = Array.ofDim[Float](buffer.limit().toInt)
      buffer.get(array)
      array
    }
  }

  implicit class ExtendedDoublePointer(buffer: DoublePointer) {
    def toArray: Array[Double] = {
      val array = Array.ofDim[Double](buffer.limit().toInt)
      buffer.get(array)
      array
    }
  }
}
