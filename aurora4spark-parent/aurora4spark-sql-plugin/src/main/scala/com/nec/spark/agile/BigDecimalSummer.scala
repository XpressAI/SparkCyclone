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

  /** This is a summer that we SSH into and run a Python script to call the VE */
  object PythonNecSSHSummer extends BigDecimalSummer {
    override def sum(nums: List[BigDecimal]): BigDecimal = {
      val cmd =
        (Seq("ssh", "a6", "/root/d/sum.sh") ++ nums.map(_.toBigInt().toString))
      try readBigDecimal(cmd.!!)
      catch {
        case e: Throwable =>
          throw new RuntimeException(s"Could not do ${cmd} due to $e", e)
      }
    }
  }

  object BundleNecSSHSummer extends BigDecimalSummer {
    override def sum(nums: List[BigDecimal]): BigDecimal =
      BundleExecutor.returningBigDecimalRemote.executeBundle(Bundle.sumBigDecimals(nums))
  }

  object BundleNecSSHSummerPurePython extends BigDecimalSummer {
    override def sum(nums: List[BigDecimal]): BigDecimal =
      BundleExecutor.returningBigDecimalRemote.executeBundle(Bundle.sumBigDecimalsPurePython(nums))
  }

}
