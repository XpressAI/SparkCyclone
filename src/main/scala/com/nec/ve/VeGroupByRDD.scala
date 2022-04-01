package com.nec.ve

import com.nec.native.CompiledVeFunction
import com.nec.ve.colvector.VeColBatch.VeBatchOfBatches
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

class VeGroupByRDD[K, T](
  verdd: VeRDD[T],
  func: CompiledVeFunction
)(override implicit val tag: ClassTag[(K, VeColBatch)], implicit val ktag: ClassTag[K], implicit val ttag: ClassTag[T])
  extends RDD[(K, VeColBatch)](verdd)
    with VeRDD[(K, VeColBatch)] {

  override val inputs: RDD[VeColBatch] = null
  val keyedInputs: RDD[(K, VeColBatch)] = computeKeyedVe()

  override protected def getPartitions: Array[Partition] = keyedInputs.partitions
  override def compute(split: Partition, context: TaskContext): Iterator[(K, VeColBatch)] = keyedInputs.iterator(split, context)

  def computeKeyedVe(): RDD[(K, VeColBatch)] = {
    verdd.inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val batchOfBatches = VeBatchOfBatches.fromVeColBatches(batches.toList)

      func.evalGrouping[K](batchOfBatches).map { case (key, colVectors) =>
        (key, VeColBatch.fromList(colVectors))
      }.iterator
    }
  }

  override def vemap[U: ClassTag](expr: universe.Expr[((K, VeColBatch)) => U]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[((K, VeColBatch)) => Boolean]): VeRDD[(K, VeColBatch)] = ???

  override def vereduce(expr: universe.Expr[((K, VeColBatch), (K, VeColBatch)) => (K, VeColBatch)]): (K, VeColBatch) = ???

  override def toRDD: RDD[(K, VeColBatch)] = keyedInputs

  override def vegroupBy[G](expr: universe.Expr[((K, VeColBatch)) => G]): VeRDD[(G, Iterable[(K, VeColBatch)])] = ???

  def vesortBy[K](f: T => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): VeRDD[T] = ???

}
