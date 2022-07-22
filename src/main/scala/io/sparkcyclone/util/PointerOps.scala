package io.sparkcyclone.util

import scala.reflect.ClassTag
import java.lang.{Long => JLong}
import org.bytedeco.javacpp._

object PointerOps {
  implicit class ExtendedPointer[T <: Pointer : ClassTag](buffer: T) {
    def nbytes: Long = {
      buffer.limit * buffer.sizeof
    }

    def slice(offset: Long, size: Long): T = {
      require(offset >= 0, "offset must be >= 0")
      require(size >= 0, "size must be >= 0")
      val klass = implicitly[ClassTag[T]].runtimeClass

      val outbuffer = klass.getConstructor(classOf[Long]).newInstance(size: JLong).asInstanceOf[T]
      Pointer.memcpy(outbuffer, buffer.position(offset * buffer.sizeof), size * buffer.sizeof)
      buffer.position(0L)
      outbuffer.position(0L)
    }

    def asBytePointer: BytePointer = {
      /*
        Set the capacity value after cast to account for the size difference
        between the source and destination pointer types (casting JavaCPP
        pointers literally copies the capacity value over as is).
      */
      new BytePointer(buffer).capacity(nbytes).position(0L)
    }

    def toHex: Array[String] = {
      asBytePointer.toArray.map { b => String.format("%02x", Byte.box(b)) }
    }

    def hexdump: String = {
      toHex.sliding(16, 16)
        .map { chunk => chunk.mkString(" ") }
        .mkString("\n")
    }
  }

  implicit class ExtendedBytePointer(buffer: BytePointer) {
    def toArray: Array[Byte] = {
      val array = Array.ofDim[Byte](buffer.limit().toInt)
      buffer.position(0L).get(array).position(0L)
      array
    }

    def as[T <: Pointer : ClassTag]: T = {
      val klass = implicitly[ClassTag[T]].runtimeClass
      val cbuffer = klass.getConstructor(classOf[Pointer]).newInstance(buffer).asInstanceOf[T]
      cbuffer.capacity(buffer.limit() / cbuffer.sizeof)
    }
  }

  implicit class ExtendedShortPointer(buffer: ShortPointer) {
    def toArray: Array[Short] = {
      val array = Array.ofDim[Short](buffer.limit().toInt)
      buffer.position(0L).get(array).position(0L)
      array
    }
  }

  implicit class ExtendedIntPointer(buffer: IntPointer) {
    def toArray: Array[Int] = {
      val array = Array.ofDim[Int](buffer.limit().toInt)
      buffer.position(0L).get(array).position(0L)
      array
    }
  }

  implicit class ExtendedLongPointer(buffer: LongPointer) {
    def toArray: Array[Long] = {
      val array = Array.ofDim[Long](buffer.limit().toInt)
      buffer.position(0L).get(array).position(0L)
      array
    }
  }

  implicit class ExtendedFloatPointer(buffer: FloatPointer) {
    def toArray: Array[Float] = {
      val array = Array.ofDim[Float](buffer.limit().toInt)
      buffer.position(0L).get(array).position(0L)
      array
    }
  }

  implicit class ExtendedDoublePointer(buffer: DoublePointer) {
    def toArray: Array[Double] = {
      val array = Array.ofDim[Double](buffer.limit().toInt)
      buffer.position(0L).get(array).position(0L)
      array
    }
  }
}
