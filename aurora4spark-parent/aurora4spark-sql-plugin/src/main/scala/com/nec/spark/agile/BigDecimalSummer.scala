package com.nec.spark.agile

import scala.sys.process._
import scala.util.Try

/** Basic class to do our initial summing of BigDecimals */
trait BigDecimalSummer extends Serializable {
  def sum(nums: List[BigDecimal]): BigDecimal
}

object BigDecimalSummer {
  object ScalaSummer extends BigDecimalSummer {
    override def sum(nums: List[BigDecimal]): BigDecimal = nums.sum
  }

  private[agile] def readBigDecimal(result: String): BigDecimal =
    Try(BigDecimal(result.trim)).toEither.fold(
      err => throw new RuntimeException(s"Could not parse input due to $err; `${result}`", err),
      num => num
    )
}
