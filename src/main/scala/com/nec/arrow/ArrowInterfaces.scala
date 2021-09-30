package com.nec.arrow

import com.nec.arrow.ArrowTransferStructures._
import org.apache.arrow.vector._
import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer
import java.nio.ByteBuffer

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.sql.util.ArrowUtilsExposed

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

  def c_nullable_double_vector(float8Vector: Float8Vector): nullable_double_vector = {
    val vc = new nullable_double_vector()
    vc.data = float8Vector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer = float8Vector.getValidityBufferAddress
    vc.count = float8Vector.getValueCount
    vc
  }

  def c_nullable_varchar_vector(varCharVector: VarCharVector): nullable_varchar_vector = {
    val vc = new nullable_varchar_vector()
    vc.data = varCharVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.offsets = varCharVector.getOffsetBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer =
      varCharVector.getValidityBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = varCharVector.getValueCount
    vc.size = varCharVector.sizeOfValueBuffer()
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

  def c_nullable_int_vector(intVector: IntVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    vc.data = intVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer = intVector.getValidityBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = intVector.getValueCount
    vc
  }

  def c_nullable_bit_vector(bitVector: BitVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(bitVector.getValueCount)

    (0 until bitVector.getValueCount).foreach{
      case idx if(!bitVector.isNull(idx)) => intVector.set(idx, bitVector.get(idx))
      case idx => intVector.setNull(idx)
    }
    vc.data = intVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer = bitVector.getValidityBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = bitVector.getValueCount
    vc
  }

  def c_nullable_int_vector(smallIntVector: SmallIntVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(smallIntVector.getValueCount)

    (0 until smallIntVector.getValueCount)

      .foreach{
        case idx if(!smallIntVector.isNull(idx)) => intVector.set(idx, smallIntVector.get(idx).toInt)
        case idx => intVector.setNull(idx)
      }
    vc.data = intVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer = smallIntVector.getValidityBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = smallIntVector.getValueCount
    vc
  }

  def c_nullable_bigint_vector(bigIntVector: BigIntVector): nullable_bigint_vector = {
    val vc = new nullable_bigint_vector()
    vc.data = bigIntVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer =
      bigIntVector.getValidityBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = bigIntVector.getValueCount
    vc
  }

  def c_nullable_date_vector(dateDayVector: DateDayVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    vc.data = dateDayVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.validityBuffer =
      dateDayVector.getValidityBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    vc.count = dateDayVector.getValueCount
    vc
  }

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def non_null_int_vector_to_intVector(input: non_null_int_vector, intVector: IntVector): Unit = {
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(intVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data, intVector.getDataBufferAddress, input.count * 4)
  }

  def non_null_bigint_vector_to_bigintVector(
    input: non_null_bigint_vector,
    bigintVector: BigIntVector
  ): Unit = {
    bigintVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(bigintVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data, bigintVector.getDataBufferAddress, input.size())
  }

  def nullable_bigint_vector_to_BigIntVector(
    input: nullable_bigint_vector,
    bigintVector: BigIntVector
  ): Unit = {
    bigintVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer,
      bigintVector.getValidityBufferAddress,
      Math.ceil(input.count / 8.0).toInt
    )
    getUnsafe.copyMemory(input.data, bigintVector.getDataBufferAddress, input.size())
  }

  def non_null_double_vector_to_float8Vector(
    input: non_null_double_vector,
    float8Vector: Float8Vector
  ): Unit = {
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    float8Vector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(float8Vector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data, float8Vector.getDataBufferAddress, input.size())
  }

  def nullable_double_vector_to_float8Vector(
    input: nullable_double_vector,
    float8Vector: Float8Vector
  ): Unit = {
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    float8Vector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer,
      float8Vector.getValidityBufferAddress,
      Math.ceil(input.count / 8.0).toInt
    )
    getUnsafe.copyMemory(input.data, float8Vector.getDataBufferAddress, input.size())
  }

  def non_null_int2_vector_to_IntVector(input: non_null_int2_vector, intVector: IntVector): Unit = {
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(intVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data, intVector.getDataBufferAddress, input.size())
  }

  def nullable_int_vector_to_IntVector(input: nullable_int_vector, intVector: IntVector): Unit = {
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    intVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer,
      intVector.getValidityBufferAddress,
      Math.ceil(input.count / 8.0).toInt
    )
    getUnsafe.copyMemory(input.data, intVector.getDataBufferAddress, input.size())
  }

  def nullable_int_vector_to_SmallIntVector(input: nullable_int_vector, smallIntVector: SmallIntVector): Unit = {
    val intVector = new IntVector("temp", ArrowUtilsExposed.rootAllocator)
    nullable_int_vector_to_IntVector(input, intVector)
    smallIntVector.setValueCount(intVector.getValueCount)
    (0 until intVector.getValueCount).foreach {
      case idx if(intVector.isNull(idx)) => smallIntVector.setNull(idx)
      case idx => smallIntVector.set(idx, intVector.get(idx).toShort)
    }
    intVector.clear()
    intVector.close()
  }

  def nullable_varchar_vector_to_VarCharVector(
    input: nullable_varchar_vector,
    varCharVector: VarCharVector
  ): Unit = {
    varCharVector.allocateNew(input.size.toLong, input.count)
    varCharVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer,
      varCharVector.getValidityBufferAddress,
      Math.ceil(input.count / 8.0).toInt
    )
    getUnsafe.copyMemory(input.data, varCharVector.getDataBufferAddress, input.size.toLong)
    getUnsafe.copyMemory(input.offsets, varCharVector.getOffsetBufferAddress, 4 * (input.count + 1))
  }

  def nullable_int_vector_to_BitVector(input: nullable_int_vector, bitVector: BitVector): Unit = {
    val intVector = new IntVector("temp", ArrowUtilsExposed.rootAllocator)
    nullable_int_vector_to_IntVector(input, intVector)
    bitVector.setValueCount(intVector.getValueCount)
    (0 until intVector.getValueCount).foreach {
      case idx if(intVector.isNull(idx)) => bitVector.setNull(idx)
      case idx => bitVector.set(idx, intVector.get(idx))
    }
    intVector.clear()
    intVector.close()
  }
}
