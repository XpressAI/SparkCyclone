package com.nec.native

import com.sun.jna.Pointer
import com.nec.CountStringsLibrary.{non_null_double_vector, non_null_int_vector, varchar_vector}
import org.apache.arrow.vector.{BitVectorHelper, Float8Vector, IntVector, VarCharVector}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import sun.nio.ch.DirectBuffer

object ArrowInterfaces {

  def non_null_int_vector_to_IntVector(input: non_null_int_vector,
                                       output: IntVector): Unit = {
    val nBytes = input.count * 4
    output.getAllocator.newReservation().reserve(nBytes)
    non_null_int_vector_to_intVector(input, output, output.getAllocator)
  }

  def non_null_double_vector_to_float8Vector(input: non_null_double_vector,
                                             output: Float8Vector): Unit = {
    val nBytes = input.count * 8
    output.getAllocator.newReservation().reserve(nBytes)

    non_null_double_vector_to_float8Vector(input, output, output.getAllocator)
  }

  def c_double_vector(float8Vector: Float8Vector): non_null_double_vector = {
    val vc = new non_null_double_vector()
    vc.data = new Pointer(
      float8Vector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    )

    vc.count = float8Vector.getValueCount
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
        new ArrowBuf(
          validityBuffer.getReferenceManager,
          null,
          input.count * 8,
          Pointer.nativeValue(input.data)
        )
      ).asJava
    )
  }
}
