package com.nec.arrow
import java.nio.{Buffer, ByteBuffer}

import com.nec.arrow.ArrowTransferStructures.{non_null_int_vector, varchar_vector, _}
import com.nec.spark.planning.SummingPlanOffHeap
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.{BitVectorHelper, IntVector, _}
import sun.nio.ch.DirectBuffer

object ArrowInterfaces {

  def non_null_int_vector_to_IntVector(input: non_null_int_vector, output: IntVector): Unit = {
    non_null_int_vector_to_intVector(input, output)
  }
  def non_null_bigint_vector_to_bigIntVector(
    input: non_null_bigint_vector,
    output: BigIntVector
  ): Unit = {
    non_null_bigint_vector_to_bigintVector(input, output)
  }

  def c_double_vector(float8Vector: Float8Vector): non_null_double_vector = {
    val vc = new non_null_double_vector()
    vc.data = float8Vector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()

    vc.count = float8Vector.getValueCount
    vc
  }

  def c_non_null_varchar_vector(varCharVector: VarCharVector): non_null_varchar_vector = {
    val vc = new non_null_varchar_vector()
    vc.data = varCharVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.offsets = varCharVector.getOffsetBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = varCharVector.getValueCount
    // Not sure if this fully correct
    vc.size = varCharVector
      .getOffsetBuffer()
      .getInt(varCharVector.getValueCount * BaseVariableWidthVector.OFFSET_WIDTH)
    vc
  }

  def non_null_varchar_vector_to_VarCharVector(
    input: non_null_varchar_vector,
    output: VarCharVector
  ): Unit = {
    nun_null_varchar_vector_to_VarCharVector(input, output, output.getAllocator)
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

  def c_bounded_data(byteBuffer: ByteBuffer, bufSize: Int): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
    vc.data = byteBuffer match {
      case direct: DirectBuffer => direct.address()
      case other =>
        val direct = ByteBuffer.allocateDirect(bufSize)
        direct.put(byteBuffer)
        direct.asInstanceOf[DirectBuffer].address()
    }
    vc.length = bufSize
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

  def non_null_int_vector_to_intVector(input: non_null_int_vector, intVector: IntVector): Unit = {
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setValidityBitToOne(intVector.getValidityBuffer, i))
    SummingPlanOffHeap.getUnsafe.copyMemory(
      input.data,
      intVector.getDataBufferAddress,
      input.count * 4
    )
  }

  def non_null_bigint_vector_to_bigintVector(
    input: non_null_bigint_vector,
    bigintVector: BigIntVector
  ): Unit = {
    bigintVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setValidityBitToOne(bigintVector.getValidityBuffer, i))
    SummingPlanOffHeap.getUnsafe.copyMemory(
      input.data,
      bigintVector.getDataBufferAddress,
      input.size()
    )
  }

  def non_null_double_vector_to_float8Vector(
    input: non_null_double_vector,
    float8Vector: Float8Vector
  ): Unit = {
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    float8Vector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setValidityBitToOne(float8Vector.getValidityBuffer, i))
    SummingPlanOffHeap.getUnsafe.copyMemory(
      input.data,
      float8Vector.getDataBufferAddress,
      input.size()
    )
  }

  def non_null_int2_vector_to_IntVector(input: non_null_int2_vector, intVector: IntVector): Unit = {
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setValidityBitToOne(intVector.getValidityBuffer, i))
    SummingPlanOffHeap.getUnsafe.copyMemory(
      input.data,
      intVector.getDataBufferAddress,
      input.size()
    )
  }

  /**
   * TODO fix allocations here; fortunately we don't use this in our initial version - this is specifically
   * to return strings from C code.
   */
  def nun_null_varchar_vector_to_VarCharVector(
    input: non_null_varchar_vector,
    varCharVector: VarCharVector,
    rootAllocator: BufferAllocator
  ): Unit = {
    val res = rootAllocator.newReservation()
    res.add(input.count)
    val validityBuffer = res.allocateBuffer()
    validityBuffer.reallocIfNeeded(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setValidityBitToOne(validityBuffer, i))
    import scala.collection.JavaConverters._
    //TODO: Not sure if that's correct. It should probably work fine,
    // but not sure if it won't cause memory leaks.

    val dataBuffer = rootAllocator.buffer(input.size)
    val offBuffer =
      rootAllocator.buffer((input.count + 1) * 4)

    val wrappedData = wrapAddress(input.data, input.size)
    val wrappedOffsets = wrapAddress(input.offsets, (input.count+1)*4)
    dataBuffer.setBytes(0, wrappedData)
    offBuffer.setBytes(0, wrappedOffsets)
    varCharVector.loadFieldBuffers(
      new ArrowFieldNode(input.count, 0),
      List(validityBuffer, offBuffer, dataBuffer).asJava
    )

  }
  def wrapAddress(addr: Long, length: Int): ByteBuffer = {
    val bb = ByteBuffer.allocateDirect(0)
    val address = classOf[Buffer].getDeclaredField("address")
    address.setAccessible(true)
    val capacity = classOf[Buffer].getDeclaredField("capacity")
    capacity.setAccessible(true)
    try {
      address.setLong(bb, addr)
      capacity.setInt(bb, length)
      bb.clear
    } catch {
      case e: IllegalAccessException =>
        throw new AssertionError(e)
    }
    bb
  }

}
