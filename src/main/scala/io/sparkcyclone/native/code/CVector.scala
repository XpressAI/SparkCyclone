package io.sparkcyclone.native.code

sealed trait CVector {
  def withNewName(str: String): CVector
  def declarePointer: String = s"${veType.cVectorType} *${name}"
  def replaceName(search: String, replacement: String): CVector
  def name: String
  def veType: VeType
}

object CVector {
  def apply(name: String, veType: VeType): CVector = {
    veType match {
      case VeString        => varChar(name)
      case o: VeScalarType => CScalarVector(name, o)
    }
  }

  def varChar(name: String): CVector = CVarChar(name)
  def double(name: String): CVector = CScalarVector(name, VeNullableDouble)
  def int(name: String): CVector = CScalarVector(name, VeNullableShort)
  def bigInt(name: String): CVector = CScalarVector(name, VeNullableLong)
}

final case class CVarChar(name: String) extends CVector {
  override def veType: VeType = VeString

  override def replaceName(search: String, replacement: String): CVector = {
    copy(name = name.replaceAllLiterally(search, replacement))
  }

  override def withNewName(str: String): CVector = copy(name = str)
}

final case class CScalarVector(name: String, veType: VeScalarType) extends CVector {
  override def replaceName(search: String, replacement: String): CVector = {
    copy(name = name.replaceAllLiterally(search, replacement))
  }

  override def withNewName(str: String): CVector = copy(name = str)
}
