package io.sparkcyclone.native.code

import org.apache.spark.sql.types._
import scala.reflect.ClassTag

sealed trait VeType {
  def makeCVector(name: String): CVector
  def containerSize: Int
  def isString: Boolean
  def cVectorType: String
  def cScalarType: String
  def cEnumValue: Long
  def scalaType: Class[_]
  def toSparkType: DataType
}

object VeType {
  final val All: Set[VeType] = Set(VeString) ++ VeScalarType.All
}

case object VeString extends VeType {
  def makeCVector(name: String): CVector = CVector.varChar(name)
  def containerSize: Int = 40
  def cVectorType: String = "nullable_varchar_vector"
  def scalaType: Class[_] = classOf[String]
  def toSparkType: DataType = StringType
  final def isString: Boolean = true

  def cScalarType: String = "int32_t"
  def cEnumValue: Long = 5
}

sealed trait VeScalarType extends VeType {
  def makeCVector(name: String): CVector = CScalarVector(name, this)
  final def containerSize: Int = 20
  final def isString: Boolean = false
  def cScalarType: String
  def cSize: Int
}

object VeScalarType {
  final val All: Set[VeScalarType] = Set(VeNullableDouble, VeNullableFloat, VeNullableInt, VeNullableShort, VeNullableLong)

  final val JvmToVeTypeMap = Map[Class[_], VeScalarType](
    classOf[Int] -> VeNullableInt,
    classOf[Long] -> VeNullableLong,
    classOf[Float] -> VeNullableFloat,
    classOf[Double] -> VeNullableDouble,
    classOf[Short] -> VeNullableShort
  )

  def fromJvmType[T : ClassTag]: VeScalarType = {
    val klass = implicitly[ClassTag[T]].runtimeClass
    JvmToVeTypeMap.get(klass) match {
      case Some(typ)  => typ
      case None       => throw new NotImplementedError(s"No corresponding VeScalarType for primitive type: ${klass}")
    }
  }
}

case object VeNullableDouble extends VeScalarType {
  def cScalarType: String = "double"
  def cVectorType: String = "nullable_double_vector"
  def cEnumValue: Long = 4
  def scalaType: Class[_] = classOf[Double]
  def toSparkType: DataType = DoubleType
  def cSize: Int = 8
}

case object VeNullableFloat extends VeScalarType {
  def cScalarType: String = "float"
  def cVectorType: String = "nullable_float_vector"
  def cEnumValue: Long = 3
  def scalaType: Class[_] = classOf[Float]
  def toSparkType: DataType = FloatType
  def cSize: Int = 4
}

case object VeNullableShort extends VeScalarType {
  // Since VE is not optimized for shorts, we use int32_t instead
  def cScalarType: String = "int32_t"
  def cVectorType: String = "nullable_short_vector"
  def cEnumValue: Long = 0
  def scalaType: Class[_] = classOf[Short]
  def toSparkType: DataType = ShortType
  def cSize: Int = 4
}

case object VeNullableInt extends VeScalarType {
  def cScalarType: String = "int32_t"
  def cVectorType: String = "nullable_int_vector"
  def cEnumValue: Long = 1
  def scalaType: Class[_] = classOf[Int]
  def toSparkType: DataType = IntegerType
  def cSize: Int = 4
}

case object VeNullableLong extends VeScalarType {
  def cScalarType: String = "int64_t"
  def cVectorType: String = "nullable_bigint_vector"
  def cEnumValue: Long = 2
  def scalaType: Class[_] = classOf[Long]
  def toSparkType: DataType = LongType
  def cSize: Int = 8
}
