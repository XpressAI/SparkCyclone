package com.nec.colvector

import com.nec.spark.agile.core._
import com.nec.util.{FixedBitSet, CallContext}
import com.nec.util.ReflectionOps._
import com.nec.ve.{VeProcess, VeProcessMetrics}
import java.nio.charset.StandardCharsets
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.compare.VectorEqualsVisitor
import org.bytedeco.javacpp._
import sun.misc.Unsafe

object ArrowVectorConversions {
  lazy val ArrowToVeScalarTypeMap: Map[Class[_ <: BaseFixedWidthVector], VeScalarType] = Map(
    classOf[Float4Vector]   -> VeNullableFloat,
    classOf[Float8Vector]   -> VeNullableDouble,
    classOf[BigIntVector]   -> VeNullableLong,
    classOf[IntVector]      -> VeNullableInt,
    classOf[DateDayVector]  -> VeNullableInt
  )

  trait ArrowFixedAllocator {
    def allocate(name: String)(implicit allocator: BufferAllocator): BaseFixedWidthVector
  }

  lazy val VeScalarToArrowAllocators: Map[VeScalarType, ArrowFixedAllocator] = Map(
    VeNullableInt -> new ArrowFixedAllocator {
      def allocate(n: String)(implicit a: BufferAllocator): BaseFixedWidthVector = new IntVector(n, a)
    },
    VeNullableLong -> new ArrowFixedAllocator {
      def allocate(n: String)(implicit a: BufferAllocator): BaseFixedWidthVector = new BigIntVector(n, a)
    },
    VeNullableFloat -> new ArrowFixedAllocator {
      def allocate(n: String)(implicit a: BufferAllocator): BaseFixedWidthVector = new Float4Vector(n, a)
    },
    VeNullableDouble -> new ArrowFixedAllocator {
      def allocate(n: String)(implicit a: BufferAllocator): BaseFixedWidthVector = new Float8Vector(n, a)
    }
  )

  implicit class ValueVectorEqualityChecks(vector: ValueVector) {
    def === (other: ValueVector): Boolean = {
      VectorEqualsVisitor.vectorEquals(vector, other)
    }
  }

  implicit class BPCVToFieldVector(input: BytePointerColVector) {
    private[colvector] lazy val numItems = input.numItems
    private[colvector] lazy val buffers = input.buffers
    private[colvector] lazy val bufferAdresses = buffers.map(_.address())

    private[colvector] def toScalarArrow(typ: VeScalarType)(implicit allocator: BufferAllocator): FieldVector = {
      val vec = VeScalarToArrowAllocators(typ).allocate(input.name)

      if (input.numItems > 0) {
        val dataSize = numItems * typ.cSize
        vec.setValueCount(numItems)

        implicitly[Unsafe].copyMemory(
          bufferAdresses(1),
          vec.getValidityBufferAddress,
          // Arrow requires 8-byte alignment
          (numItems / 64.0).ceil.toInt * 8
        )

        implicitly[Unsafe].copyMemory(bufferAdresses(0), vec.getDataBufferAddress, dataSize)
      }

      vec
    }

    private[colvector] def toShortArrow(implicit allocator: BufferAllocator): SmallIntVector = {
      val vec = new SmallIntVector(input.name, allocator)

      if (numItems > 0) {
        vec.setValueCount(numItems)

        // Cast the data buffer as int[] and read directly into SmallIntVector
        val buffer = new IntPointer(buffers(0))
        for (i <- 0 until numItems) {
          vec.set(i, buffer.get(i).toShort)
        }

        implicitly[Unsafe].copyMemory(
          bufferAdresses(1),
          vec.getValidityBufferAddress,
          // Arrow requires 8-byte alignment
          (numItems / 64.0).ceil.toInt * 8
        )
      }

      vec
    }

    private[colvector] def toVarCharArrow(implicit allocator: BufferAllocator): VarCharVector = {
      val vec = new VarCharVector(input.name, allocator)

      if (numItems > 0) {
        // Allocate and set count
        vec.allocateNew(buffers(0).capacity(), numItems)
        vec.setValueCount(numItems)

        val dataBuffer = buffers(0)
        val startsBuffer = new IntPointer(buffers(1))
        val lensBuffer = new IntPointer(buffers(2))
        val bitset = FixedBitSet.from(buffers(3))

        for (i <- 0 until numItems) {
          // Get the validity bit at position i
          if (bitset.get(i)) {
            // Read starts and lens as byte offsets (they are stored in BytePointerColVector as int32_t offsets)
            val start = startsBuffer.get(i.toLong) * 4
            val len = lensBuffer.get(i.toLong) * 4

            // Copy the chunk of the dataBuffer
            val bytes = new Array[Byte](len)
            for (i <- 0 until len) {
              bytes(i) = dataBuffer.get(start + i)
            }

            // Parse it as UTF-32LE string and convert to UTF-8 bytes
            val utf8bytes = new String(bytes,  "UTF-32LE").getBytes(StandardCharsets.UTF_8)

            // Write to Arrow
            vec.set(i, utf8bytes)
          }
        }
      }

      vec
    }

    def toArrowVector(implicit allocator: BufferAllocator): FieldVector = {
      input.veType match {
        case VeNullableShort =>
          // Specialize this case because Int values in VeNullableShort need to be cast to Short
          toShortArrow

        case typ: VeScalarType if VeScalarToArrowAllocators.contains(typ) =>
          toScalarArrow(typ)

        case VeString =>
          toVarCharArrow

        case other =>
          throw new NotImplementedError(s"Conversion of BytePointerColVector of VeType ${other} to Arrow not supported")
      }
    }
  }

  implicit class ValueVectorToBPCV(vector: ValueVector) {
    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      vector match {
        case vec: SmallIntVector =>
          // Specialize this case because values need to be cast to Int first
          vec.toBytePointerColVector

        case vec: BaseFixedWidthVector if ArrowToVeScalarTypeMap.contains(vec.getClass) =>
          vec.toBytePointerColVector

        case vec: VarCharVector =>
          vec.toBytePointerColVector

        case other =>
          throw new NotImplementedError(s"Conversion of ${other.getClass.getName} to BytePointerColVector not supported")
      }
    }
  }

  implicit class BaseFixedWidthVectorToBPCV(vector: BaseFixedWidthVector) {
    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      val veType = ArrowToVeScalarTypeMap.get(vector.getClass) match {
        case Some(stype) => stype
        case None => throw new IllegalArgumentException(s"No corresponding VeType found for Arrow type '${vector.getClass.getName}'")
      }

      BytePointerColVector(
        source,
        vector.getName,
        veType,
        vector.getValueCount,
        Seq(
          new BytePointer(vector.getDataBuffer.nioBuffer),
          new BytePointer(vector.getValidityBuffer.nioBuffer)
        )
      )
    }
  }

  implicit class SmallIntVectorToBPCV(vector: SmallIntVector) {
    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      // Convert the values to Ints
      val buffer = new IntPointer(vector.getValueCount.toLong)
      for (i <- 0 until vector.getValueCount) {
        val x = if (vector.isNull(i)) 0 else vector.get(i).toInt
        buffer.put(i.toLong, x)
      }

      BytePointerColVector(
        source,
        vector.getName,
        VeNullableShort,
        vector.getValueCount,
        Seq(
          /*
            Cast to BytePointer and manually set the capacity value to account
            for the size difference between the two pointer types (casting
            JavaCPP pointers literally copies the capacity value over as is).
          */
          new BytePointer(buffer).capacity(vector.getValueCount.toLong * 4),
          new BytePointer(vector.getValidityBuffer.nioBuffer)
        )
      )
    }
  }

  implicit class VarCharVectorToBPCV(vector: VarCharVector) {
    private[colvector] def constructBuffers: (BytePointer, BytePointer, BytePointer) = {
      /*
        Compute the buffer length

        NOTE: We compute this manually because `vector.getDataBuffer.readableBytes`
        appears to return 0 unconditionally.
      */
      val bufferlen = {
        var len = 0
        for (i <- 0 until vector.getValueCount) {
          if (! vector.isNull(i)) {
            len += vector.getObject(i).toString.getBytes("UTF-32LE").size
          }
        }
        len
      }

      val dataBuffer = new BytePointer(bufferlen)
      val startsBuffer = new IntPointer(vector.getValueCount.toLong)
      val lensBuffer = new IntPointer(vector.getValueCount.toLong)

      var pos = 0
      for (i <- 0 until vector.getValueCount) {
        if (! vector.isNull(i)) {
          // Get the string as bytes
          val bytes = vector.getObject(i).toString.getBytes("UTF-32LE")

          // Write the starts and lens as int32_t offsets
          startsBuffer.put(i.toLong, pos / 4)
          lensBuffer.put(i.toLong, bytes.size / 4)

          // Write to BytePointer
          bytes.foreach { b =>
            // Copy byte over - note that can't use bulk `put()` because it always assumes position 0 in the destination buffer
            dataBuffer.put(pos.toLong, b)
            pos += 1
          }
        }
      }

      (
        dataBuffer,
        /*
          Cast to BytePointer and manually set the capacity value to account for
          the size difference between the two pointer types (casting JavaCPP
          pointers literally copies the capacity value over as is).
        */
        new BytePointer(startsBuffer).capacity(vector.getValueCount.toLong * 4),
        new BytePointer(lensBuffer).capacity(vector.getValueCount.toLong * 4)
      )
    }

    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      val (dataBuffer, startsBuffer, lensBuffer) = constructBuffers

      BytePointerColVector(
        source,
        vector.getName,
        VeString,
        vector.getValueCount,
        Seq(
          dataBuffer,
          startsBuffer,
          lensBuffer,
          new BytePointer(vector.getValidityBuffer.nioBuffer)
        )
      )
    }
  }

  implicit class ValueVectorToVeColVector(vector: ValueVector) {
    def toVeColVector(implicit veProcess: VeProcess,
                      source: VeColVectorSource,
                      context: CallContext,
                      metrics: VeProcessMetrics): VeColVector = {
      vector.toBytePointerColVector.toVeColVector
    }
  }
}
