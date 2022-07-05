package org.apache.spark.sql

import io.sparkcyclone.spark.agile.core.VeType
import org.apache.spark.sql.types.{DataType, StringType, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

final class UserDefinedVeType extends UserDefinedType[VeType] {
  override def sqlType: DataType = StringType

  override def deserialize(datum: Any): VeType = VeType.All
    .find(_.cVectorType == new String(datum.asInstanceOf[UTF8String].getBytes))
    .getOrElse(sys.error(s"Could not deserialize '${datum}'"))

  override def serialize(obj: VeType): Any =
    UTF8String.fromString(obj.cVectorType)

  override def userClass: Class[VeType] = classOf[VeType]
}
