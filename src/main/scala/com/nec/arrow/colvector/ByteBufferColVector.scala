package com.nec.arrow.colvector

import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString}
import com.nec.ve.VeProcess
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtilsExposed.RichSmallIntVector

import java.nio.ByteBuffer

/**
 * Storage of a col vector as serialized Arrow buffers, that are in ByteBuffers.
 * We use Option[] because the `container` has no ByteBuffer.
 */
final case class ByteBufferColVector(underlying: GenericColVector[Option[ByteBuffer]]) {
  def toVeColVector()(implicit veProcess: VeProcess, _source: VeColVectorSource): VeColVector =
    VeColVector(
      transferBuffersToVe()
        .map(_.getOrElse(-1))
    ).newContainer()

  def transferBuffersToVe()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource
  ): GenericColVector[Option[Long]] =
    underlying
      .map(_.map(bb => veProcess.putBuffer(bb)))
      .copy(source = source)

  def serializeBuffers(): ByteArrayColVector =
    ByteArrayColVector(
      underlying.copy(
        container = None,
        buffers = underlying
          .map(_.map(bb => {
            try bb.array()
            catch {
              case _: UnsupportedOperationException =>
                val size = bb.capacity()
                val target: Array[Byte] = Array.fill(size)(-1)
                bb.get(target)
                target
            }
          }))
          .buffers
      )
    )

}

object ByteBufferColVector {

  def fromArrowVector(
    valueVector: ValueVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    valueVector match {
      case float8Vector: Float8Vector     => fromFloat8Vector(float8Vector)
      case bigIntVector: BigIntVector     => fromBigIntVector(bigIntVector)
      case intVector: IntVector           => fromIntVector(intVector)
      case varCharVector: VarCharVector   => fromVarcharVector(varCharVector)
      case dateDayVector: DateDayVector   => fromDateDayVector(dateDayVector)
      case smallIntVector: SmallIntVector => fromSmallIntVector(smallIntVector)
      case other                          => sys.error(s"Not supported to convert from ${other.getClass}")
    }

  def fromBigIntVector(
    bigIntVector: BigIntVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = bigIntVector.getValueCount,
        name = bigIntVector.getName,
        veType = VeScalarType.VeNullableLong,
        container = None,
        buffers = List(
          Option(bigIntVector.getDataBuffer.nioBuffer()),
          Option(bigIntVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromIntVector(dirInt: IntVector)(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = dirInt.getValueCount,
        name = dirInt.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(dirInt.getDataBuffer.nioBuffer()),
          Option(dirInt.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromSmallIntVector(
    smallDirInt: SmallIntVector
  )(implicit source: VeColVectorSource): ByteBufferColVector = {
    val intVector = smallDirInt.toIntVector
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = smallDirInt.getValueCount,
        name = smallDirInt.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(intVector.getDataBuffer.nioBuffer()),
          Option(intVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )
  }

  def fromDateDayVector(
    dateDayVector: DateDayVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = dateDayVector.getValueCount,
        name = dateDayVector.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(dateDayVector.getDataBuffer.nioBuffer()),
          Option(dateDayVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromFloat8Vector(
    float8Vector: Float8Vector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = float8Vector.getValueCount,
        name = float8Vector.getName,
        veType = VeScalarType.VeNullableDouble,
        container = None,
        buffers = List(
          Option(float8Vector.getDataBuffer.nioBuffer()),
          Option(float8Vector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromVarcharVector(
    varcharVector: VarCharVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = varcharVector.getValueCount,
        name = varcharVector.getName,
        veType = VeString,
        container = None,
        buffers = List(
          Option(varcharVector.getDataBuffer.nioBuffer()),
          Option(varcharVector.getOffsetBuffer.nioBuffer()),
          Option(varcharVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = Some(varcharVector.getDataBuffer.nioBuffer().capacity())
      )
    )

}
