package com.nec.ve

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.merge.MergeFunction
import com.nec.ve.colvector.VeColBatch.VeBatchOfBatches
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.Partition
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}

import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

class VeConcatGroups[K: ClassTag, T: ClassTag](
  shuffled: ShuffledRDD[K, VeColBatch, VeColBatch]
)(implicit val tag: ClassTag[(K, Iterable[T])]) extends RDD[(K, Iterable[T])](shuffled) with VeRDD[(K, Iterable[T])] {

  override val inputs: RDD[VeColBatch] = null
  val concatInputs: RDD[(K, VeColBatch)] = computeMergeVe()

  override protected def getPartitions: Array[Partition] = concatInputs.partitions

  def computeMergeVe(): RDD[(K, VeColBatch)] = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val funcName = s"merge_${Math.abs(hashCode())}"
    val code = MergeFunction.apply(funcName, List(dataType))
    val func = code.toCFunction
    val veFunc = code.toVeFunction
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders).toString

    shuffled.mapPartitions { batchIter =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val batches = batchIter.toList
      if (batches.nonEmpty) {
        batches.groupBy(_._1).map { case (key, grouped) =>
          val libRef = veProcess.loadLibrary(Paths.get(compiledPath))
          val batchOfBatches = VeBatchOfBatches.fromVeColBatches(grouped.map(_._2))
          val merged = veProcess.executeMultiIn(libRef, funcName, batchOfBatches, veFunc.namedResults)
          (key, VeColBatch.fromList(merged))
        }.toIterator
      } else {
        Iterator()
      }
    }
  }

  def toRDD : RDD[(K, Iterable[T])] = {
    concatInputs.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
      val klass = implicitly[ClassTag[T]].runtimeClass

      batches.map { case (key, veColBatch) =>
        val arrowBatch = veColBatch.toArrowColumnarBatch()
        val array = if (klass == classOf[Int]) {
          arrowBatch.column(0).getInts(0, arrowBatch.numRows())
        } else if (klass == classOf[Long]) {
          arrowBatch.column(0).getLongs(0, arrowBatch.numRows())
        } else if (klass == classOf[Float]) {
          arrowBatch.column(0).getFloats(0, arrowBatch.numRows())
        } else {
          arrowBatch.column(0).getDoubles(0, arrowBatch.numRows())
        }
        (key, array.toSeq.asInstanceOf[Seq[T]])
      }
    }
  }

  override def vemap[U: ClassTag](expr: universe.Expr[((K, Iterable[T])) => U]): VeRDD[U] = ???

  override def veflatMap[U: ClassTag](expr: universe.Expr[((K, Iterable[T])) => TraversableOnce[U]]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[((K, Iterable[T])) => Boolean]): VeRDD[(K, Iterable[T])] = ???

  override def vereduce(expr: universe.Expr[((K, Iterable[T]), (K, Iterable[T])) => (K, Iterable[T])]): (K, Iterable[T]) = ???


}
