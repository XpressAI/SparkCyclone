package com.nec.ve

import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.colvector.VeColBatch.VeBatchOfBatches
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

class VeGroupByRDD[K: Ordering : ClassTag, T: ClassTag](
  verdd: VeRDD[T],
  func: CFunction2,
  soPath: String,
  outputs: List[CVector])
  (implicit val tag: ClassTag[(K, VeColBatch)])
  extends RDD[(K, VeColBatch)](verdd)
    with VeRDD[(K, VeColBatch)] {

  override val inputs: RDD[VeColBatch] = null
  lazy val keyedInputs: RDD[(K, VeColBatch)] = computeKeyedVe()

  override protected def getPartitions: Array[Partition] = keyedInputs.partitions


  def computeKeyedVe(): RDD[(K, VeColBatch)] = {
    verdd.inputs.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val libRef = veProcess.loadLibrary(Paths.get(soPath))

      val batchOfBatches = VeBatchOfBatches.fromVeColBatches(batches.toList)

      val results = veProcess.executeGrouping[K](libRef, func.name, batchOfBatches, outputs)

      results.map { case (key, colVectors) =>
        (key, VeColBatch.fromList(colVectors))
      }.iterator
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, VeColBatch)] = {
    keyedInputs.iterator(split, context)
  }

  override def vemap[U: ClassTag](expr: universe.Expr[((K, VeColBatch)) => U]): VeRDD[U] = ???

  override def veflatMap[U: ClassTag](expr: universe.Expr[((K, VeColBatch)) => TraversableOnce[U]]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[((K, VeColBatch)) => Boolean]): VeRDD[(K, VeColBatch)] = ???

  override def vereduce(expr: universe.Expr[((K, VeColBatch), (K, VeColBatch)) => (K, VeColBatch)]): (K, VeColBatch) = ???

  override def toRDD: RDD[(K, VeColBatch)] = keyedInputs
}
