package com.nec.ve.exchange

import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVector
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

trait VeExchangeStrategy extends Serializable {
  def exchange(rdd: RDD[(Int, List[VeColVector])], cleanUpInput: Boolean)(implicit
    originalCallingContext: OriginalCallingContext
  ): RDD[List[VeColVector]]

  object Implicits extends Serializable {
    implicit class RichKeyedRDDL(rdd: RDD[(Int, List[VeColVector])]) {
      def exchangeBetweenVEs(cleanUpInput: Boolean)(implicit
        originalCallingContext: OriginalCallingContext
      ): RDD[List[VeColVector]] =
        exchange(rdd, cleanUpInput)
    }
  }
}

object VeExchangeStrategy {

  def sparkShuffleBased: VeExchangeStrategy = SparkShuffleBasedExchangeStrategy

  def vectorHostBased: VeExchangeStrategy = VectorHostBasedExchangeStrategy

  def exchangeLS(rdd: RDD[(Int, List[VeColVector])])(implicit
    veProcess: VeProcess
  ): RDD[List[VeColVector]] = rdd.repartitionByKey().map(_._2)

  private[exchange] implicit class IntKeyedRDD[V: ClassTag](rdd: RDD[(Int, V)]) {
    def repartitionByKey(): RDD[(Int, V)] =
      new ShuffledRDD[Int, V, V](rdd, new HashPartitioner(rdd.partitions.length))
  }

  def fromString(string: String): Option[VeExchangeStrategy] =
    PartialFunction.condOpt(string) {
      case "spark"       => sparkShuffleBased
      case "vector-host" => vectorHostBased
    }

}
