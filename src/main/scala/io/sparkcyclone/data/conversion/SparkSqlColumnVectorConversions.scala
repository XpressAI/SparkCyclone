package io.sparkcyclone.data.conversion

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.vector.BytePointerColVector
import io.sparkcyclone.native.code._
import io.sparkcyclone.util.FixedBitSet
import io.sparkcyclone.util.PointerOps._
import io.sparkcyclone.util.ReflectionOps._
import java.nio.charset.StandardCharsets
import org.apache.arrow.vector.FieldVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector}
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
    private[conversion] def veScalarType: VeScalarType = {
      SparkToVeScalarTypeMap.get(vector.dataType) match {
        case Some(x)  => x
        case _        => throw new NotImplementedError(s"No corresponding VeType for SparkSQL DataType: ${vector.dataType}")
      }
    }

    private[conversion] def validityBuffer(size: Int): BytePointer = {
      val bitset = new FixedBitSet(size)
      for (i <- 0 until size) {
        bitset.set(i, !vector.isNullAt(i))
      }

      bitset.toBytePointer
    }

    private[conversion] def scalarDataBuffer(size: Int): BytePointer = {
      //ColumnVectorUtils.populate()
      val buffer = vector.dataType match {
        case IntegerType =>
          val ptr = new IntPointer(size.toLong)
          if (!vector.hasNull) {
            ptr.put(vector.getInts(0, size), 0, size)
          } else {
            // Check for nullability first is required, or else a value fetch on a row marked as null will throw an exception
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getInt(i)))
          }
          ptr

        case LongType =>
          val ptr = new LongPointer(size.toLong)
          if (!vector.hasNull) {
            ptr.put(vector.getLongs(0, size), 0, size)
          } else {
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getLong(i)))
          }
          ptr

        case FloatType =>
          val ptr = new FloatPointer(size.toLong)

          if (!vector.hasNull) {
            ptr.put(vector.getFloats(0, size), 0, size)
          } else {
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getFloat(i)))
          }
          ptr

        case DoubleType =>
          val ptr = new DoublePointer(size.toLong)
          if (!vector.hasNull) {
            ptr.put(vector.getDoubles(0, size), 0, size)
          } else {
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getDouble(i)))
          }
          ptr

        case ShortType =>
          val ptr = new IntPointer(size.toLong)
          if (!vector.hasNull) {
            ptr.put(vector.getInts(0, size), 0, size)
          } else {
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getShort(i).toInt))
          }
          ptr

        case TimestampType =>
          val ptr = new LongPointer(size.toLong)
          if (!vector.hasNull) {
            ptr.put(vector.getLongs(0, size), 0, size)
          } else {
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getLong(i)))
          }
          ptr

        case DateType =>
          val ptr = new IntPointer(size.toLong)
          if (!vector.hasNull) {
            ptr.put(vector.getInts(0, size), 0, size)
          } else {
            (0 until size).foreach(i => ptr.put(i.toLong, if (vector.isNullAt(i)) 0 else vector.getInt(i)))
          }
          ptr
      }

      buffer.asBytePointer
    }

    private[conversion] def scalarToBPCV(name: String, size: Int)(implicit source: VeColVectorSource): BytePointerColVector = {
      BytePointerColVector(
        source,
        name,
        veScalarType,
        size,
        Seq(
          scalarDataBuffer(size),
          validityBuffer(size)
        )
      )
    }

    private[conversion] def varCharToBPCV(name: String, size: Int)(implicit source: VeColVectorSource): BytePointerColVector = {
      import ArrayTConversions._

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
        source,
        name,
        VeString,
        size,
        Seq(
          dataBuffer,
          startsBuffer,
          lensBuffer,
          validityBuffer(size)
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
          throw new NotImplementedError(s"Conversion of SparkSQL DataType '${other}' to BytePointerColVector not supported")
      }
    }
  }

  implicit class SparkSqlColumnVectorToArrow(vector: ColumnVector) {
    def extractArrowVector: Option[FieldVector] = {
      Option(vector).collect {
        case x: ArrowColumnVector =>
          x.readPrivate
            .accessor
            .vector
            .obj
            .asInstanceOf[FieldVector]
      }
    }
  }

  object HasFieldVector {
    def unapply(vector: ColumnVector): Option[FieldVector] = {
      PartialFunction.condOpt(vector.readPrivate.accessor.vector.obj) {
        case x: FieldVector => x
      }
    }
  }
}
