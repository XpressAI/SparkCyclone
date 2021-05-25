package com.nec.native

import com.sun.jna.Pointer
import com.nec.CountStringsLibrary.{non_null_int_vector, varchar_vector}
import org.apache.arrow.vector.{BitVectorHelper, IntVector, VarCharVector}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import sun.nio.ch.DirectBuffer

object ArrowInterfaces {

  def non_null_int_vector_to_IntVector(input: non_null_int_vector, output: IntVector): Unit = {
    val nBytes = input.count * 4
    output.getAllocator.newReservation().reserve(nBytes)
    non_null_int_vector_to_intVector(input, output, output.getAllocator)
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
}
