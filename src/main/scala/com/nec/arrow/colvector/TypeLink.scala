package com.nec.arrow.colvector

import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{
  BaseFixedWidthVector,
  BigIntVector,
  DateDayVector,
  Float8Vector,
  IntVector
}
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DateType,
  DoubleType,
  IntegerType,
  LongType,
  ShortType,
  TimestampType
}
import org.apache.spark.sql.vectorized.ColumnVector

sealed trait TypeLink {
  type ArrowType <: BaseFixedWidthVector
  def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType
  def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit
  def veScalarType: VeScalarType
}

object TypeLink {
  private case object DoubleTypeLink extends TypeLink {
    override type ArrowType = Float8Vector

    override def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType =
      new Float8Vector(name, bufferAllocator)

    override def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit =
      if (from.isNullAt(idx)) to.setNull(idx)
      else to.set(idx, from.getDouble(idx))

    override def veScalarType: VeScalarType = VeScalarType.veNullableDouble
  }

  private case object LongTypeLink extends TypeLink {
    override type ArrowType = BigIntVector

    override def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType =
      new BigIntVector(name, bufferAllocator)

    override def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit =
      if (from.isNullAt(idx)) to.setNull(idx)
      else to.set(idx, from.getLong(idx))

    override def veScalarType: VeScalarType = VeScalarType.veNullableLong
  }

  private case object BooleanTypeLink extends TypeLink {
    override type ArrowType = IntVector

    override def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType =
      new IntVector(name, bufferAllocator)

    override def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit =
      if (from.isNullAt(idx)) to.setNull(idx)
      else {
        to.set(idx, if (from.getBoolean(idx)) 1 else 0)
      }

    override def veScalarType: VeScalarType = VeScalarType.veNullableInt
  }

  private case object IntegerTypeLink extends TypeLink {
    override type ArrowType = IntVector

    override def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType =
      new IntVector(name, bufferAllocator)

    override def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit =
      if (from.isNullAt(idx)) to.setNull(idx)
      else to.set(idx, from.getInt(idx))

    override def veScalarType: VeScalarType = VeScalarType.veNullableInt
  }

  private case object ShortTypeLink extends TypeLink {
    override type ArrowType = IntVector

    override def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType =
      new IntVector(name, bufferAllocator)

    override def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit =
      if (from.isNullAt(idx)) to.setNull(idx)
      else to.set(idx, from.getShort(idx).toInt)

    override def veScalarType: VeScalarType = VeScalarType.veNullableShort
  }

  private case object DateTypeLink extends TypeLink {
    override type ArrowType = DateDayVector

    override def makeArrow(name: String)(implicit bufferAllocator: BufferAllocator): ArrowType =
      new DateDayVector(name, bufferAllocator)

    override def transfer(idx: Int, from: ColumnVector, to: ArrowType): Unit =
      if (from.isNullAt(idx)) to.setNull(idx)
      else to.set(idx, from.getInt(idx))

    override def veScalarType: VeScalarType = VeScalarType.veNullableInt
  }

  val SparkToArrow: Map[DataType, TypeLink] = Map(
    BooleanType -> BooleanTypeLink,
    IntegerType -> IntegerTypeLink,
    ShortType -> ShortTypeLink,
    DoubleType -> DoubleTypeLink,
    LongType -> LongTypeLink,
    TimestampType -> LongTypeLink,
    DateType -> DateTypeLink
  )

  val VeToArrow: Map[VeScalarType, TypeLink] = Map(
    VeScalarType.VeNullableDouble -> DoubleTypeLink,
    VeScalarType.VeNullableInt -> IntegerTypeLink,
    VeScalarType.VeNullableLong -> LongTypeLink
  )

  val ArrowToVe: Map[Class[_ <: BaseFixedWidthVector], TypeLink] = Map(
    classOf[Float8Vector] -> DoubleTypeLink,
    classOf[BigIntVector] -> LongTypeLink,
    classOf[IntVector] -> IntegerTypeLink,
    classOf[DateDayVector] -> DateTypeLink
  )
}
