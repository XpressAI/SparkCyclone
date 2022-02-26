package com.nec.ve.colvector

import com.nec.arrow.ArrowTransferStructures.{
  nullable_bigint_vector,
  nullable_double_vector,
  nullable_int_vector,
  nullable_short_vector
}
import com.nec.arrow.VeArrowTransfers.{
  nullableBigintVectorToBytePointer,
  nullableDoubleVectorToBytePointer,
  nullableIntVectorToBytePointer,
  nullableShortVectorToBytePointer
}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import org.bytedeco.javacpp.BytePointer

sealed trait VeArrowTypeMapping {
  def toBytePointer(count: Int, data: Long, validityBuffer: Long): BytePointer
}
object VeArrowTypeMapping {

  private case object ForDouble extends VeArrowTypeMapping {
    override def toBytePointer(count: Int, data: Long, validityBuffer: Long): BytePointer = {
      val vcv = new nullable_double_vector()
      vcv.count = count
      vcv.data = data
      vcv.validityBuffer = validityBuffer
      nullableDoubleVectorToBytePointer(vcv)
    }
  }
  private case object ForInt extends VeArrowTypeMapping {
    override def toBytePointer(count: Int, data: Long, validityBuffer: Long): BytePointer = {
      val vcv = new nullable_int_vector()
      vcv.count = count
      vcv.data = data
      vcv.validityBuffer = validityBuffer
      nullableIntVectorToBytePointer(vcv)
    }
  }
  private case object ForShort extends VeArrowTypeMapping {
    override def toBytePointer(count: Int, data: Long, validityBuffer: Long): BytePointer = {
      val vcv = new nullable_short_vector()
      vcv.count = count
      vcv.data = data
      vcv.validityBuffer = validityBuffer
      nullableShortVectorToBytePointer(vcv)
    }
  }
  private case object ForLong extends VeArrowTypeMapping {
    override def toBytePointer(count: Int, data: Long, validityBuffer: Long): BytePointer = {
      val vcv = new nullable_bigint_vector()
      vcv.count = count
      vcv.data = data
      vcv.validityBuffer = validityBuffer
      nullableBigintVectorToBytePointer(vcv)
    }
  }

  val VeTypeToBytePointer: Map[VeScalarType, VeArrowTypeMapping] = Map(
    VeScalarType.VeNullableDouble -> ForDouble,
    VeScalarType.VeNullableInt -> ForInt,
    VeScalarType.VeNullableShort -> ForShort,
    VeScalarType.VeNullableLong -> ForLong
  )

}
