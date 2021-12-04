package org.apache.spark.sql

import com.nec.spark.agile.CFunctionGeneration.VeType
import org.apache.spark.sql.types.{DataType, StringType, UserDefinedType}

final class UserDefinedVeType extends UserDefinedType[VeType] {
  override def sqlType: DataType = StringType

  override def deserialize(datum: Any): VeType = VeType.All
    .find(_.cVectorType == datum.asInstanceOf[String])
    .getOrElse(sys.error(s"Could not deserialize '${datum}'"))

  override def serialize(obj: VeType): Any = obj.cVectorType

  override def userClass: Class[VeType] = classOf[VeType]
}
