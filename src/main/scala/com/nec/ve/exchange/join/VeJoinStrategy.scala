package com.nec.ve.exchange.join

import com.nec.ve.VeColBatch
import com.nec.ve.colvector.VeColBatch.VeColVector
import org.apache.spark.rdd.RDD

trait VeJoinStrategy extends Serializable {

  final def joinExchangeLB(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)],
    cleanUpInput: Boolean
  ): RDD[(List[VeColVector], List[VeColVector])] =
    join(
      left = left.map { case (k, b) => k -> b.cols },
      right = right.map { case (k, b) => k -> b.cols },
      cleanUpInput = cleanUpInput
    )

  def join(
    left: RDD[(Int, List[VeColVector])],
    right: RDD[(Int, List[VeColVector])],
    cleanUpInput: Boolean
  ): RDD[(List[VeColVector], List[VeColVector])]

}

object VeJoinStrategy {
  def fromString(string: String): Option[VeJoinStrategy] =
    PartialFunction.condOpt(string) {
      case "host"  => hostBasedJoin
      case "spark" => sparkBasedJoin
    }

  def hostBasedJoin: VeJoinStrategy = VectorHostBasedJoinStrategy
  def sparkBasedJoin: VeJoinStrategy = SparkShuffleBasedVeJoinStrategy
}
