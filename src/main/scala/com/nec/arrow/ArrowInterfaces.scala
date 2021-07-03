package com.nec.arrow
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.BitVectorHelper
import com.nec.arrow.ArrowTransferStructures.non_null_int_vector
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector._
import com.sun.jna.Pointer
import com.nec.arrow.ArrowTransferStructures.varchar_vector
import org.apache.arrow.memory.BufferAllocator
import sun.nio.ch.DirectBuffer
import org.apache.arrow.vector.IntVector
import com.nec.arrow.ArrowTransferStructures._

import java.nio.ByteBuffer

object ArrowInterfaces {

  def non_null_int_vector_to_IntVector(input: non_null_int_vector, output: IntVector): Unit = {
    val nBytes = input.count * 4
    output.getAllocator.newReservation().reserve(nBytes)
    non_null_int_vector_to_intVector(input, output, output.getAllocator)
  }

  def non_null_double_vector_to_float8Vector(
    input: non_null_double_vector,
    output: Float8Vector
  ): Unit = {
    val nBytes = input.count * 8
    output.getAllocator.newReservation().reserve(nBytes)

    non_null_double_vector_to_float8Vector(input, output, output.getAllocator)
  }

  def c_double_vector(float8Vector: Float8Vector): non_null_double_vector = {
    val vc = new non_null_double_vector()
    vc.data = float8Vector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()

    vc.count = float8Vector.getValueCount
    vc
  }

  def c_bounded_string(string: String): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
    vc.data = ByteBuffer
      .allocateDirect(string.length)
      .put(string.getBytes())
      .asInstanceOf[DirectBuffer]
      .address()
    vc.length = string.length
    vc
  }

  def c_int2_vector(intVector: IntVector): non_null_int2_vector = {
    val vc = new non_null_int2_vector()
    vc.data = intVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = intVector.getValueCount
    vc
  }

  def c_varchar_vector(varCharVector: VarCharVector): varchar_vector = {
    val vc = new varchar_vector()
    vc.data = varCharVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.offsets = varCharVector.getOffsetBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = varCharVector.getValueCount
    vc
  }

  def non_null_int_vector_to_intVector(
    input: non_null_int_vector,
    intVector: IntVector,
    rootAllocator: BufferAllocator
  ): Unit = {

    /** Set up the validity buffer -- everything is valid here * */
    val res = rootAllocator.newReservation()
    res.add(input.count)
    val validityBuffer = res.allocateBuffer()
    validityBuffer.reallocIfNeeded(input.count.toLong)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(validityBuffer, i))

    import scala.collection.JavaConverters._
    intVector.loadFieldBuffers(
      new ArrowFieldNode(input.count.toLong, 0),
      List(
        validityBuffer,
        new ArrowBuf(
          validityBuffer.getReferenceManager,
          null,
          input.count * 4,
          Pointer.nativeValue(input.data)
        )
      ).asJava
    )
  }

  def non_null_double_vector_to_float8Vector(
    input: non_null_double_vector,
    intVector: Float8Vector,
    rootAllocator: BufferAllocator
  ): Unit = {

    /** Set up the validity buffer -- everything is valid here * */
    val res = rootAllocator.newReservation()
    res.add(input.count)
    val validityBuffer = res.allocateBuffer()
    validityBuffer.reallocIfNeeded(input.count.toLong)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(validityBuffer, i))
    import scala.collection.JavaConverters._
    intVector.loadFieldBuffers(
      new ArrowFieldNode(input.count.toLong, 0),
      List(
        validityBuffer,
        new ArrowBuf(validityBuffer.getReferenceManager, null, input.count * 8, input.data)
      ).asJava
    )
  }
}
