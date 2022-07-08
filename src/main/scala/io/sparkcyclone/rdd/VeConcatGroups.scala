package io.sparkcyclone.rdd

import io.sparkcyclone.native.transpiler.CompiledVeFunction
import io.sparkcyclone.spark.agile.merge.MergeFunction
import io.sparkcyclone.data.vector.VeBatchOfBatches
import io.sparkcyclone.data.vector.{VeColVector, VeColBatch}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

class VeConcatGroups[K: universe.TypeTag, T: universe.TypeTag](
  shuffled: ShuffledRDD[K, VeColBatch, VeColBatch]
)(implicit val typeTag: universe.TypeTag[(K, Iterable[T])]) extends RDD[(K, Iterable[T])](shuffled)(ClassTag(typeTag.mirror.runtimeClass(typeTag.tpe))) with VeRDD[(K, Iterable[T])] {
  override val inputs: RDD[VeColBatch] = null
  val concatInputs: RDD[(K, VeColBatch)] = computeMergeVe()

  override def compute(split: Partition, context: TaskContext): Iterator[(K, Iterable[T])] = {
    import io.sparkcyclone.spark.SparkCycloneExecutorPlugin.{source, veProcess}
   import io.sparkcyclone.util.CallContextOps._

    val batches = concatInputs.iterator(split, context)
    batches.map { case (key, veColBatch) =>
      val res = (key, veColBatch.toCPUSeq[T])
      veColBatch.free()
      res
    }
  }

  override protected def getPartitions: Array[Partition] = concatInputs.partitions

  def computeMergeVe(): RDD[(K, VeColBatch)] = {
    import io.sparkcyclone.native.transpiler.SyntaxTreeOps._

    val outputTypes = implicitly[universe.TypeTag[T]].tpe.toVeTypes

    val funcName = s"merge_${outputTypes.mkString("_")}_2"
    val code = MergeFunction(funcName, outputTypes)
    val func = CompiledVeFunction(code.toCFunction, code.toVeFunction.outputs, null)

    shuffled.mapPartitions { batchIter =>
     import io.sparkcyclone.util.CallContextOps._
      val batches = batchIter.toList
      if (batches.nonEmpty) {
        batches.groupBy(_._1).map { case (key, grouped) =>
          val batchOfBatches = VeBatchOfBatches(grouped.map(_._2))
          val merged: Seq[VeColVector] = func.evalMultiInFunction(batchOfBatches)
          (key, VeColBatch(merged))
        }.toIterator
      } else {
        Iterator()
      }
    }
  }

  override def toRDD: RDD[(K, Iterable[T])] = {
    concatInputs.mapPartitions { batches =>
      batches.map { case (key, veColBatch) =>
        import io.sparkcyclone.spark.SparkCycloneExecutorPlugin.{source, veProcess}
        import io.sparkcyclone.util.CallContextOps._

        val array = veColBatch.toCPUSeq[T]
        veColBatch.free()

        (key, array)
      }
    }
  }

  override def vemap[U: universe.TypeTag](expr: universe.Expr[((K, Iterable[T])) => U]): VeRDD[U] = ???

  //override def veflatMap[U: ClassTag](expr: universe.Expr[((K, Iterable[T])) => TraversableOnce[U]]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[((K, Iterable[T])) => Boolean]): VeRDD[(K, Iterable[T])] = ???

  override def vereduce(expr: universe.Expr[((K, Iterable[T]), (K, Iterable[T])) => (K, Iterable[T])]): (K, Iterable[T]) = ???

  override def vegroupBy[G: universe.TypeTag](expr: universe.Expr[((K, Iterable[T])) => G]): VeRDD[(G, Iterable[(K, Iterable[T])])] = ???

  override def vesortBy[G: universe.TypeTag](expr: universe.Expr[((K, Iterable[T])) => G], ascending: Boolean, numPartitions: Int): VeRDD[(K, Iterable[T])] = ???
}
