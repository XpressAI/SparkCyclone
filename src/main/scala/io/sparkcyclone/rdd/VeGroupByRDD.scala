package io.sparkcyclone.rdd

import io.sparkcyclone.native.transpiler.CompiledVeFunction
import io.sparkcyclone.data.vector.VeBatchOfBatches
import io.sparkcyclone.data.vector.{VeColVector, VeColBatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

class VeGroupByRDD[G, T](
  verdd: VeRDD[T],
  func: CompiledVeFunction
)(override implicit val typeTag: universe.TypeTag[(G, VeColBatch)])
  extends RDD[(G, VeColBatch)](verdd)(ClassTag(typeTag.mirror.runtimeClass(typeTag.tpe)))
    with VeRDD[(G, VeColBatch)] {

  override val inputs: RDD[VeColBatch] = null
  val keyedInputs: RDD[(G, VeColBatch)] = computeKeyedVe()

  override protected def getPartitions: Array[Partition] = keyedInputs.partitions
  override def compute(split: Partition, context: TaskContext): Iterator[(G, VeColBatch)] = keyedInputs.iterator(split, context)

  def computeKeyedVe(): RDD[(G, VeColBatch)] = {
    implicit val g: ClassTag[G] = func.types.output.tag.asInstanceOf[ClassTag[G]]
    verdd.inputs.mapPartitions { batches =>
     import io.sparkcyclone.util.CallContextOps._
      val batchesList = batches.toList
      if (batchesList.isEmpty) {
        Nil.toIterator
      } else {
        val batchOfBatches = VeBatchOfBatches(batchesList)

        func.evalGrouping[G](batchOfBatches).map { case (key, colVectors) =>
          (key, VeColBatch(colVectors))
        }.iterator
      }
    }
  }

  override def vemap[U: universe.TypeTag](expr: universe.Expr[((G, VeColBatch)) => U]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[((G, VeColBatch)) => Boolean]): VeRDD[(G, VeColBatch)] = ???

  override def vereduce(expr: universe.Expr[((G, VeColBatch), (G, VeColBatch)) => (G, VeColBatch)]): (G, VeColBatch) = ???

  override def toRDD: RDD[(G, VeColBatch)] = keyedInputs

  override def vegroupBy[K: universe.TypeTag](expr: universe.Expr[((G, VeColBatch)) => K]): VeRDD[(K, Iterable[(G, VeColBatch)])] = ???

  override def vesortBy[K: universe.TypeTag](expr: universe.Expr[((G, VeColBatch)) => K], ascending: Boolean, numPartitions: Int): VeRDD[(G, VeColBatch)] = ???
}
