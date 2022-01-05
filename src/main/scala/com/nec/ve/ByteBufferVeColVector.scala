package com.nec.ve

import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString}
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource, VectorEngineLocation}
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtilsExposed.RichSmallIntVector
import org.apache.spark.sql.vectorized.ColumnVector

object ByteBufferVeColVector {

  def fromVectorColumn(source: ColumnVector)(implicit
    veColVectorSource: VeColVectorSource
  ): ByteBufferVeColVector = fromArrowVector(source.getArrowValueVector)

  def fromArrowVector(
    valueVector: ValueVector
  )(implicit veColVectorSource: VeColVectorSource): ByteBufferVeColVector = {
    valueVector match {
      case float8Vector: Float8Vector     => fromFloat8Vector(float8Vector)
      case bigIntVector: BigIntVector     => fromBigIntVector(bigIntVector)
      case intVector: IntVector           => fromIntVector(intVector)
      case varCharVector: VarCharVector   => fromVarcharVector(varCharVector)
      case dateDayVector: DateDayVector   => fromDateDayVector(dateDayVector)
      case smallIntVector: SmallIntVector => fromSmallIntVector(smallIntVector)
      case other                          => sys.error(s"Not supported to convert from ${other.getClass}")
    }
  }

  implicit final class RichByteBufferVeColVector(vec: ByteBufferVeColVector) {
    def transferBuffersToVe()(implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): ColVector[Option[VectorEngineLocation]] =
      vec
        .map(_.map(bb => VectorEngineLocation(veProcess.putBuffer(bb))))
        .copy(source = source)

    def serializeBuffers(): MaybeByteArrayColVector =
      vec.copy(
        containerLocation = None,
        buffers = vec.buffers.map(_.map(bb => {
          try bb.array()
          catch {
            case _: UnsupportedOperationException =>
              val size = bb.capacity()
              val target: Array[Byte] = Array.fill(size)(-1)
              bb.get(target)
              target
          }
        }))
      )
  }

  def fromBigIntVector(
    bigIntVector: BigIntVector
  )(implicit source: VeColVectorSource): ByteBufferVeColVector =
    ColVector(
      source = source,
      numItems = bigIntVector.getValueCount,
      name = bigIntVector.getName,
      veType = VeScalarType.VeNullableLong,
      containerLocation = None,
      buffers = List(
        Some(bigIntVector.getDataBuffer.nioBuffer()),
        Some(bigIntVector.getValidityBuffer.nioBuffer())
      ),
      variableSize = None
    )

  def fromIntVector(dirInt: IntVector)(implicit source: VeColVectorSource): ByteBufferVeColVector =
    ColVector(
      source = source,
      numItems = dirInt.getValueCount,
      name = dirInt.getName,
      veType = VeScalarType.VeNullableInt,
      containerLocation = None,
      buffers =
        List(Some(dirInt.getDataBuffer.nioBuffer()), Some(dirInt.getValidityBuffer.nioBuffer())),
      variableSize = None
    )

  def fromSmallIntVector(
    smallDirInt: SmallIntVector
  )(implicit source: VeColVectorSource): ByteBufferVeColVector = {
    val intVector = smallDirInt.toIntVector
    ColVector(
      source = source,
      numItems = smallDirInt.getValueCount,
      name = smallDirInt.getName,
      veType = VeScalarType.VeNullableInt,
      containerLocation = None,
      buffers = List(
        Some(intVector.getDataBuffer.nioBuffer()),
        Some(intVector.getValidityBuffer.nioBuffer())
      ),
      variableSize = None
    )
  }

  def fromDateDayVector(
    dateDayVector: DateDayVector
  )(implicit source: VeColVectorSource): ByteBufferVeColVector =
    ColVector(
      source = source,
      numItems = dateDayVector.getValueCount,
      name = dateDayVector.getName,
      veType = VeScalarType.VeNullableInt,
      containerLocation = None,
      buffers = List(
        Some(dateDayVector.getDataBuffer.nioBuffer()),
        Some(dateDayVector.getValidityBuffer.nioBuffer())
      ),
      variableSize = None
    )

  def fromFloat8Vector(
    float8Vector: Float8Vector
  )(implicit source: VeColVectorSource): ByteBufferVeColVector =
    ColVector(
      source = source,
      numItems = float8Vector.getValueCount,
      name = float8Vector.getName,
      veType = VeScalarType.VeNullableDouble,
      containerLocation = None,
      buffers = List(
        Some(float8Vector.getDataBuffer.nioBuffer()),
        Some(float8Vector.getValidityBuffer.nioBuffer())
      ),
      variableSize = None
    )

  def fromVarcharVector(
    varcharVector: VarCharVector
  )(implicit source: VeColVectorSource): ByteBufferVeColVector =
    ColVector(
      source = source,
      numItems = varcharVector.getValueCount,
      name = varcharVector.getName,
      veType = VeString,
      containerLocation = None,
      buffers = List(
        Some(varcharVector.getDataBuffer.nioBuffer()),
        Some(varcharVector.getOffsetBuffer.nioBuffer()),
        Some(varcharVector.getValidityBuffer.nioBuffer())
      ),
      variableSize = Some(varcharVector.getDataBuffer.nioBuffer().capacity())
    )
}
