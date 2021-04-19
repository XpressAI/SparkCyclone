package com.nec.spark.agile

import scala.sys.process._
import scala.util.Try

/** Basic class to do our initial summing of BigDecimals */
trait BigDecimalSummer {
  def sum(nums: List[BigDecimal]): BigDecimal
}

object BigDecimalSummer {
  def scalaSummer: BigDecimalSummer = _.sum

  private[agile] def readBigDecimal(result: String): BigDecimal =
    Try(BigDecimal(result.trim)).toEither.fold(
      err => throw new RuntimeException(s"Could not parse input due to $err; `${result}`", err),
      num => num
    )

  /** This is a summer that we SSH into and run a Python script to call the VE */
  def pythonNecSummer: BigDecimalSummer =
    (nums: List[BigDecimal]) => {
      val cmd =
        (Seq("ssh", "a6", "/root/sum.sh") ++ nums.map(_.toBigInt().toString))
      try readBigDecimal(cmd.!!)
      catch {
        case e: Throwable =>
          throw new RuntimeException(s"Could not do ${cmd} due to $e", e)
      }
    }

  /** TODO this would actually compile our own app onto VE */
  def directVeSummer: BigDecimalSummer = _ => sys.error("Not yet implemented")

}
