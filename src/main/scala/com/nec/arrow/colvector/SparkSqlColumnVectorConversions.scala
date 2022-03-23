package com.nec.arrow.colvector

import com.nec.spark.agile.core._
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.spark.sql.types._
import java.nio.charset.StandardCharsets
import java.util.BitSet
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp._

object SparkSqlColumnVectorConversions {
  val SparkToVeScalarTypeMap = Map[DataType, VeScalarType](
    IntegerType    -> VeNullableInt,
    ShortType      -> VeNullableShort,
    LongType       -> VeNullableLong,
    FloatType      -> VeNullableFloat,
    DoubleType     -> VeNullableDouble,
    TimestampType  -> VeNullableLong,
    DateType       -> VeNullableInt,
  )

  implicit class SparkSqlColumnVectorToBPCV(vector: ColumnVector) {
    private[colvector] def veScalarType: VeScalarType = {
      SparkToVeScalarTypeMap.get(vector.dataType) match {
        case Some(x)  => x
        case _        => throw new NotImplementedError(s"No corresponding VeType for SparkSQL DataType: ${vector.dataType}")
      }
    }

    private[colvector] def validityBuffer(size: Int): BytePointer = {
      // Compute the bitset
      val bitset = new BitSet(size)
      for (i <- 0 until size) {
        bitset.set(i, vector.isNullAt(i))
      }

      // Fetch the byte array representation of the bitset
      val bytes = bitset.toByteArray

      // Copy byte array over to BytePointer
      val buffer = new BytePointer(bytes.size.toLong)
      buffer.put(bytes, 0, bytes.size)
    }

    private[colvector] def scalarDataBuffer(size: Int): BytePointer = {
      val buffer = vector.dataType match {
        case IntegerType =>
          val ptr = new IntPointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getInt(i)))
          ptr

        case LongType =>
          val ptr = new LongPointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getLong(i)))
          ptr

        case FloatType =>
          val ptr = new FloatPointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getFloat(i)))
          ptr

        case DoubleType =>
          val ptr = new DoublePointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getDouble(i)))
          ptr

        case ShortType =>
          val ptr = new IntPointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getShort(i).toInt))
          ptr

        case TimestampType =>
          val ptr = new LongPointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getLong(i)))
          ptr

        case DateType =>
          val ptr = new IntPointer(size.toLong)
          (0 until size).foreach(i => ptr.put(i.toLong, vector.getInt(i)))
          ptr
      }

      new BytePointer(buffer).capacity(size.toLong * veScalarType.cSize)
    }

    private[colvector] def scalarToBPCV(name: String, size: Int)(implicit source: VeColVectorSource): BytePointerColVector = {
      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = size,
          name = name,
          veType = veScalarType,
          container = None,
          buffers = List(
            Option(scalarDataBuffer(size)),
            Option(validityBuffer(size))
          ),
          variableSize = None
        )
      )
    }

    private[colvector] def varCharToBPCV(name: String, size: Int)(implicit source: VeColVectorSource): BytePointerColVector = {
      import com.nec.arrow.colvector.ArrayTConversions._

      // Construct UTF-32lE Array[Array[Byte]]
      val bytesAA = 0.until(size).toArray.map { i =>
        if (vector.isNullAt(i)) {
          Array[Byte]()
        } else {
          new String(vector.getUTF8String(i).getBytes, StandardCharsets.UTF_8).getBytes("UTF-32LE")
        }
      }

      // Extract the buffers from Array[Array[Byte]]
      val (dataBuffer, startsBuffer, lensBuffer) = bytesAA.constructBuffers

      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = size,
          name = name,
          veType = VeString,
          container = None,
          buffers = List(
            Option(dataBuffer),
            Option(startsBuffer),
            Option(lensBuffer),
            Option(validityBuffer(size))
          ),
          variableSize = Some(dataBuffer.limit().toInt / 4)
        )
      )
    }

    def toBytePointerColVector(name: String, size: Int)(implicit source: VeColVectorSource): BytePointerColVector = {
      vector.dataType match {
        case dtype if SparkToVeScalarTypeMap.contains(dtype) =>
          scalarToBPCV(name, size)

        case StringType =>
          varCharToBPCV(name, size)

        case other =>
          throw new NotImplementedError(s"SparkSQL DataType not supported: ${other}")
      }
    }
  }
}
