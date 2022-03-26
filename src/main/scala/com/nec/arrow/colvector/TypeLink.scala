package com.nec.arrow.colvector

import com.nec.spark.agile.core._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector

sealed trait TypeLink {
  type ArrowType <: BaseFixedWidthVector
  def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType
  def veScalarType: VeScalarType
}

object TypeLink {
  private case object FloatTypeLink extends TypeLink {
    override type ArrowType = Float4Vector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new Float4Vector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableFloat
  }

  private case object DoubleTypeLink extends TypeLink {
    override type ArrowType = Float8Vector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new Float8Vector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableDouble
  }

  private case object LongTypeLink extends TypeLink {
    override type ArrowType = BigIntVector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new BigIntVector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableLong
  }

  private case object BooleanTypeLink extends TypeLink {
    override type ArrowType = IntVector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new IntVector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableInt
  }

  private case object IntegerTypeLink extends TypeLink {
    override type ArrowType = IntVector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new IntVector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableInt
  }

  private case object ShortTypeLink extends TypeLink {
    override type ArrowType = IntVector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new IntVector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableShort
  }

  private case object DateTypeLink extends TypeLink {
    override type ArrowType = DateDayVector

    override def makeArrow(name: String)(implicit allocator: BufferAllocator): ArrowType = {
      new DateDayVector(name, allocator)
    }

    override def veScalarType: VeScalarType = VeNullableInt
  }

  val VeToArrow: Map[VeScalarType, TypeLink] = Map(
    VeNullableInt -> IntegerTypeLink,
    VeNullableLong -> LongTypeLink,
    VeNullableFloat -> FloatTypeLink,
    VeNullableDouble -> DoubleTypeLink,
  )

  val ArrowToVe: Map[Class[_ <: BaseFixedWidthVector], TypeLink] = Map(
    classOf[Float4Vector] -> FloatTypeLink,
    classOf[Float8Vector] -> DoubleTypeLink,
    classOf[BigIntVector] -> LongTypeLink,
    classOf[IntVector] -> IntegerTypeLink,
    classOf[DateDayVector] -> DateTypeLink
  )
}
