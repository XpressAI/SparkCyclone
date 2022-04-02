package com.nec.ve

import com.nec.native.CompiledVeFunction
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.merge.MergeFunction
import com.nec.util.DateTimeOps.ExtendedInstant
import com.nec.ve.colvector.VeColBatch.{VeBatchOfBatches, VeColVector}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.{Partition, TaskContext}

import java.time.Instant
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

class VeConcatGroups[K: ClassTag, T: ClassTag](
  shuffled: ShuffledRDD[K, VeColBatch, VeColBatch]
)(implicit val tag: ClassTag[(K, Iterable[T])]) extends RDD[(K, Iterable[T])](shuffled) with VeRDD[(K, Iterable[T])] {
  override val inputs: RDD[VeColBatch] = null
  val concatInputs: RDD[(K, VeColBatch)] = computeMergeVe()

  override def compute(split: Partition, context: TaskContext): Iterator[(K, Iterable[T])] = {
    val batches = concatInputs.iterator(split, context)
    batches.map { case (key, veColBatch) =>
      val array = veColBatch.toArray[T](0)
      (key, array.toSeq)
    }
  }

  override protected def getPartitions: Array[Partition] = concatInputs.partitions

  def computeMergeVe(): RDD[(K, VeColBatch)] = {
    val dataType = veType(implicitly[ClassTag[T]])

    val funcName = s"merge_${dataType.toString}_2"
    val code = MergeFunction(funcName, List(dataType))
    val func = CompiledVeFunction(code.toCFunction, code.toVeFunction.namedResults, null)

    shuffled.mapPartitions { batchIter =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
      val batches = batchIter.toList
      if (batches.nonEmpty) {
        batches.groupBy(_._1).map { case (key, grouped) =>
          val batchOfBatches = VeBatchOfBatches.fromVeColBatches(grouped.map(_._2))
          val merged: List[VeColVector] = func.evalMultiInFunction(batchOfBatches)
          (key, VeColBatch.fromList(merged))
        }.toIterator
      } else {
        Iterator()
      }
    }
  }

  def toRDD: RDD[(K, Iterable[T])] = {
    concatInputs.mapPartitions { batches =>
      batches.map { case (key, veColBatch) =>
        val array = veColBatch.toArray[T](0)
        (key, array.toSeq)
      }
    }
  }

  override def vemap[U: ClassTag](expr: universe.Expr[((K, Iterable[T])) => U]): VeRDD[U] = ???

  //override def veflatMap[U: ClassTag](expr: universe.Expr[((K, Iterable[T])) => TraversableOnce[U]]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[((K, Iterable[T])) => Boolean]): VeRDD[(K, Iterable[T])] = ???

  override def vereduce(expr: universe.Expr[((K, Iterable[T]), (K, Iterable[T])) => (K, Iterable[T])]): (K, Iterable[T]) = ???

  override def vegroupBy[G](expr: universe.Expr[((K, Iterable[T])) => G]): VeRDD[(G, Iterable[(K, Iterable[T])])] = ???

  override def vesortBy[G](expr: universe.Expr[((K, Iterable[T])) => G], ascending: Boolean, numPartitions: Int): VeRDD[(K, Iterable[T])] = ???
}
