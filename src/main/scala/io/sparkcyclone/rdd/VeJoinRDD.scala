package io.sparkcyclone.rdd

import io.sparkcyclone.native.transpiler.CompiledVeFunction
import io.sparkcyclone.spark.codegen.join.SimpleEquiJoinFunction
import io.sparkcyclone.data.vector.VeBatchOfBatches
import io.sparkcyclone.data.vector.{VeColVector, VeColBatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{TypeTag, reify}

object VeJoinRDD {
  def apply[K: TypeTag, V: TypeTag, W: TypeTag](leftRdd: VeRDD[(K, V)], rightRdd: VeRDD[(K, W)]): VeJoinRDD[(K, V), (K, W), (K, V, W)] = {
    val left = leftRdd.vegroupBy(reify { a: (K, V) => a._1 }).asInstanceOf[VeConcatGroups[K, V]].concatInputs
    val right = rightRdd.vegroupBy(reify { a: (K, W) => a._1}).asInstanceOf[VeConcatGroups[K, W]].concatInputs

    val kTag = implicitly[TypeTag[K]]
    val exchanged = VeRDDOps.joinExchange(left, right, cleanUpInput = true)(ClassTag(kTag.mirror.runtimeClass(kTag.tpe)))

    new VeJoinRDD(exchanged)
  }
}

class VeJoinRDD[IN: TypeTag, OUT: TypeTag, T](
  rdd: RDD[(Iterable[VeColBatch], Iterable[VeColBatch])]
)(implicit val typeTag: universe.TypeTag[T]) extends RDD[T](rdd)(ClassTag(typeTag.mirror.runtimeClass(typeTag.tpe))) with VeRDD[T] {
  override val inputs: RDD[VeColBatch] = null
  val joinedInputs: RDD[VeColBatch] = computeJoinVe()

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{source, veProcess}
   import io.sparkcyclone.util.CallContextOps._

    val batches = joinedInputs.iterator(split, context)
    batches.flatMap { veColBatch =>
      val res = veColBatch.toCPUSeq[T]
      veColBatch.free()
      res
    }
  }

  override protected def getPartitions: Array[Partition] = joinedInputs.partitions

  def computeJoinVe(): RDD[VeColBatch] = {
    import io.sparkcyclone.native.transpiler.SyntaxTreeOps._

    val leftInputTypes = implicitly[universe.TypeTag[IN]].tpe.toVeTypes
    val rightInputTypes = implicitly[universe.TypeTag[OUT]].tpe.toVeTypes

    val funcName = s"join_l_${leftInputTypes.mkString("_")}_r_${rightInputTypes.mkString("_")}"

    val joiner = SimpleEquiJoinFunction(funcName, leftInputTypes, rightInputTypes)

    val func = CompiledVeFunction(joiner.toCFunction, joiner.outputs.toList, null)

    rdd.mapPartitions { tupleIterator =>
     import io.sparkcyclone.util.CallContextOps._

      val (leftBatchesIter, rightBatchesIter) = tupleIterator.fold((Seq.empty, Seq.empty)){ case ((accLeft, accRight), (left, right)) =>
        (accLeft ++ left, accRight ++ right)
      }

      val leftBatches = leftBatchesIter.toList
      val rightBatches = rightBatchesIter.toList

      (leftBatches, rightBatches) match {
        case (Nil, _) => Iterator.empty
        case (_, Nil) => Iterator.empty
        case _ =>
          val leftBatchesBatch = VeBatchOfBatches(leftBatches)
          val rightBatchesBatch = VeBatchOfBatches(rightBatches)

          val outputBatch = func.evalJoinFunction(leftBatchesBatch, rightBatchesBatch)
          Iterator.single(VeColBatch(outputBatch))
      }
    }
  }

  override def toRDD: RDD[T] = {
    joinedInputs.mapPartitions { batches =>
      import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{source, veProcess}
     import io.sparkcyclone.util.CallContextOps._

      batches.flatMap { veColBatch =>
        val res = veColBatch.toCPUSeq[T]
        veColBatch.free()
        res
      }
    }
  }

  override def vemap[U: universe.TypeTag](expr: universe.Expr[T => U]): VeRDD[U] = ???

  //override def veflatMap[U: ClassTag](expr: universe.Expr[((K, Iterable[T])) => TraversableOnce[U]]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[T  => Boolean]): VeRDD[T] = ???

  override def vereduce(expr: universe.Expr[(T, T) => T]): T = ???

  override def vegroupBy[G: universe.TypeTag](expr: universe.Expr[T => G]): VeRDD[(G, Iterable[T])] = ???

  override def vesortBy[G: universe.TypeTag](expr: universe.Expr[T => G], ascending: Boolean, numPartitions: Int): VeRDD[T] = ???
}
