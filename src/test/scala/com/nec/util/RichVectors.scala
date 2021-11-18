package com.nec.util

import org.apache.arrow.vector.{BigIntVector, BitVectorHelper, DateDayVector, Float8Vector, IntVector, VarCharVector}

import java.time.LocalDate

object RichVectors {
  implicit class RichFloat8(float8Vector: Float8Vector) {
    def toList: List[Double] = (0 until float8Vector.getValueCount).map(float8Vector.get).toList

    def toListNullable: List[Option[Double]] = (0 until float8Vector.getValueCount).map {
      case idx if (BitVectorHelper.get(float8Vector.getValidityBuffer, idx) == 1) =>
        Some(float8Vector.get(idx))
      case _ => None
    }.toList

    def toListSafe: List[Option[Double]] =
      (0 until float8Vector.getValueCount)
        .map(idx => if (float8Vector.isNull(idx)) None else Option(float8Vector.get(idx)))
        .toList
  }

  implicit class RichDateVector(dateDayVector: DateDayVector) {
    def toList: List[LocalDate] = (0 until dateDayVector.getValueCount)
      .map(dateDayVector.get)
      .map(i => LocalDate.ofEpochDay(i))
      .toList
  }

  implicit class RichVarCharVector(varCharVector: VarCharVector) {
    def toList: List[String] = (0 until varCharVector.getValueCount).view
      .map(varCharVector.get)
      .map(bytes => new String(bytes, "UTF-8"))
      .toList

    def toListSafe: List[Option[String]] =
      (0 until varCharVector.getValueCount)
        .map(idx =>
          if (varCharVector.isNull(idx)) None
          else Option(new String(varCharVector.get(idx), "UTF-8"))
        )
        .toList
  }

  implicit class RichBigIntVector(bigIntVector: BigIntVector) {
    def toList: List[Long] = (0 until bigIntVector.getValueCount).map(bigIntVector.get).toList
  }

  implicit class RichIntVector(IntVector: IntVector) {
    def toList: List[Int] = (0 until IntVector.getValueCount).map(IntVector.get).toList
    def toListSafe: List[Option[Int]] = (0 until IntVector.getValueCount)
      .map(idx => if (IntVector.isNull(idx)) None else Some(IntVector.get(idx)))
      .toList
  }
}
