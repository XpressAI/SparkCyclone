package com.nec.ve

import com.nec.native.{CompiledVeFunction, CppTranspiler}
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.util.DateTimeOps._
import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
import com.nec.ve.serializer.VeSerializer
import org.apache.arrow.memory.RootAllocator
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel

import java.time.Instant
import scala.collection.immutable.NumericRange
import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror, universe}
import scala.tools.reflect.ToolBox

object VeRDD {
  implicit class VeRichRDD[T: ClassTag](rdd: RDD[T]) {
    def toVeRDD: VeRDD[T] = new BasicVeRDD[T](rdd)
  }

  implicit class VeRichSparkContext(sc: SparkContext) {
    def veParallelize(range: NumericRange.Inclusive[Long]): VeRDD[Long] = {
      SequenceVeRDD.makeSequence(sc, range.start, range.end)
    }
  }

  def vemap_impl[U: c.WeakTypeTag, T: c.WeakTypeTag](c: whitebox.Context)(f: c.Expr[T => U]): c.Expr[VeRDD[U]] = {
    import c.universe._
    val self = c.prefix
    val x = q"${self}.vemap(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[U]](x)
  }

  def veflatMap_impl[U: c.WeakTypeTag, T: c.WeakTypeTag](c: whitebox.Context)(f: c.Expr[T => TraversableOnce[U]]): c.Expr[VeRDD[U]] = {
    import c.universe._
    val self = c.prefix
    val x = q"${self}.veflatMap(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[U]](x)
  }

  def vefilter_impl[T](c: whitebox.Context)(f: c.Expr[(T) => Boolean]): c.Expr[VeRDD[T]] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vefilter(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[T]](x)
  }

  def vereduce_impl[T](c: whitebox.Context)(f: c.Expr[(T, T) => T]): c.Expr[T] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vereduce(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[T](x)
  }

  /*def vegroupBy_impl[K: Ordering: ClassTag, T](c: whitebox.Context)(f: c.Expr[T => K]): c.Expr[KeyedVeRDD[K, T, (K, T)]] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vegroupBy(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[KeyedVeRDD[K, T, (K, T)]](x)
  }*/
}

/*abstract class KeyedVeRDD[K: Ordering : ClassTag, V: ClassTag, P <: Product2[K, V] : ClassTag](
  prev: VeRDD[P]
) extends Serializable {
  private val ordering = implicitly[Ordering[K]]

  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = {
    // TODO: SHM shuffle
    //new ShuffledVeRDD[K, V, V](this, partitioner).setKeyOrdering(ordering)
    ???
  }

  /* inputs are not defined in a KeyedVeRDD */
  val inputs: RDD[VeColBatch] = ???

  val keyedInputs: RDD[(K, Iterable[VeColBatch])] = computeKeyedVe()

  def computeKeyedVe(): RDD[(K, Iterable[VeColBatch])]

  def vemap[U: ClassTag](expr: universe.Expr[P => U]): VeRDD[U] = ???

  def veflatMap[U: ClassTag](expr: universe.Expr[P => TraversableOnce[U]]): VeRDD[U] = {
    null
  }

  def vefilter(expr: universe.Expr[P => Boolean]): VeRDD[P] = ???

  def vegroupBy[K2: Ordering](expr: universe.Expr[P => K2]): KeyedVeRDD[K2, P, (K2, P)] = ???

  def vereduce(expr: universe.Expr[(P, P) => P]): P = ???
}*/

trait VeRDD[T] extends RDD[T] {

  import VeRDD._

  @transient protected val toolbox: ToolBox[universe.type] = currentMirror.mkToolBox()

  implicit val tag: ClassTag[T]

  val inputs: RDD[VeColBatch]

  def map[U](f: T => U): RDD[U] = macro vemap_impl[U, T]

  def flatMap[U](f: T => TraversableOnce[U]): VeRDD[U] = macro veflatMap_impl[U, T]

  override def reduce(f: (T, T) => T): T = macro vereduce_impl[T]

  override def filter(f: T => Boolean): VeRDD[T] = macro vefilter_impl[T]
  //def groupBy[K: Ordering: ClassTag](f: T => K): KeyedVeRDD[K, T, (K, T)] = macro vegroupBy_impl[K, T]


  def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U]

  def veflatMap[U: ClassTag](expr: Expr[T => TraversableOnce[U]]): VeRDD[U]

  def vefilter(expr: Expr[T => Boolean]): VeRDD[T]

  def vegroupBy[K: Ordering : ClassTag](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = ???

  def vereduce(expr: Expr[(T, T) => T]): T

  def toRDD: RDD[T]

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    val batches = inputs.iterator(split, context)

    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
    val klass = implicitly[ClassTag[T]].runtimeClass

    batches.flatMap { veColBatch =>
      val arrowBatch = veColBatch.toArrowColumnarBatch()
      val array = if (klass == classOf[Int]) {
        arrowBatch.column(0).getInts(0, arrowBatch.numRows())
      } else if (klass == classOf[Long]) {
        arrowBatch.column(0).getLongs(0, arrowBatch.numRows())
      } else if (klass == classOf[Float]) {
        arrowBatch.column(0).getFloats(0, arrowBatch.numRows())
      } else if (klass == classOf[Double]) {
        arrowBatch.column(0).getDoubles(0, arrowBatch.numRows())
      } else if (klass == classOf[Instant]) {
        arrowBatch.column(0).getLongs(0, arrowBatch.numRows()).map(ExtendedInstant.fromFrovedisDateTime)
      } else {
        throw new NotImplementedError(s"Cannot extract Array[T] from ColumnarBatch for T = ${klass}")
      }
      array.toSeq.asInstanceOf[Seq[T]]
    }
  }

  override protected def getPartitions: Array[Partition] = inputs.partitions
}

abstract class ChainedVeRDD[T](
  verdd: VeRDD[_],
  func: CompiledVeFunction
)(implicit val tag: ClassTag[T]) extends RDD[T](verdd) with VeRDD[T] {
  override val inputs: RDD[VeColBatch] = computeVe()

  def computeVe(): RDD[VeColBatch] = {
    verdd.inputs.mapPartitions { batches =>
      println(s"${this.getClass.getName}: RDD(${this.id})")

      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
      func.evalFunctionOnBatch(batches)
    }
  }

  override def toRDD : RDD[T] = {
    inputs.mapPartitions { batches =>
      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
      val klass = tag.runtimeClass
      batches.flatMap { veColBatch =>
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
        array.toSeq.asInstanceOf[Seq[T]]
      }
    }
  }


  def vereduce(expr: Expr[(T, T) => T]): T = {
    val mappedResults = inputs.mapPartitions { batches =>
      func.evalFunctionOnBatch(batches)
    }

    val klass = tag.runtimeClass

    // transpile f to C
    val newFunc = CppTranspiler.transpileReduce(expr)

    val reduceResults = mappedResults.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      newFunc.evalFunctionOnBatch(batches)
    }

    val ret = reduceResults.mapPartitions { batches =>
      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val r = batches.map { veColBatch =>
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
        array
      }
      r
    }

    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]
    ret.asInstanceOf[RDD[Array[T]]].collect().flatten.reduce(f)
  }

  override def vefilter(expr: Expr[T => Boolean]): VeRDD[T] = {
    // transpile f to C
    val newFunc = CppTranspiler.transpileFilter(expr)
    new FilteredVeRDD[T](this, newFunc)
  }

  override def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U] = {
    val klass = tag.runtimeClass

    // transpile f to C
    val newFunc = CppTranspiler.transpileMap(expr)
    new MappedVeRDD(this, newFunc)
  }

  override def veflatMap[U: ClassTag](expr: Expr[T => TraversableOnce[U]]): VeRDD[U] = ???

  override def vegroupBy[K: Ordering: ClassTag](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)

    val mapped = new VeGroupByRDD(this, newFunc)
    val out = new ShuffledRDD[K, VeColBatch, VeColBatch](mapped, new HashPartitioner(this.partitions.length))
    out.setSerializer(new VeSerializer(sparkContext.getConf, true))
    new VeConcatGroups(out)
  }
}

class BasicVeRDD[T](
  rdd: RDD[T]
)(implicit val tag: ClassTag[T])  extends RDD[T](rdd.sparkContext, List(new OneToOneDependency(rdd))) with VeRDD[T] {
  val inputs: RDD[VeColBatch] = rdd.mapPartitionsWithIndex { case (index, valsIter) =>
    import com.nec.arrow.colvector.ArrayTConversions._
    import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    val valsArray = valsIter.toArray
    val klass = implicitly[ClassTag[T]].runtimeClass
    val veVector = if (klass == classOf[Int]) {
      valsArray.asInstanceOf[Array[Int]].toBytePointerColVector(s"inputs-${index}").toVeColVector()
    } else if (klass == classOf[Long]) {
      valsArray.asInstanceOf[Array[Long]].toBytePointerColVector(s"inputs-${index}").toVeColVector()
    } else if (klass == classOf[Float]) {
      valsArray.asInstanceOf[Array[Float]].toBytePointerColVector(s"inputs-${index}").toVeColVector()
    } else if (klass == classOf[Double]) {
      valsArray.asInstanceOf[Array[Double]].toBytePointerColVector(s"inputs-${index}").toVeColVector()
    } else if (klass == classOf[Instant]) {
      valsArray.asInstanceOf[Array[Instant]]
        .map(_.toFrovedisDateTime)
        .toBytePointerColVector(s"inputs-${index}")
        .toVeColVector()
    } else {
      throw new NotImplementedError(s"Cannot convert Array[T] to VeColVector for T = ${klass}")
    }

    val batch = VeColBatch.fromList(List(veVector))
    Iterator(batch)
  }

  // Trigger caching of VeColBatches
  if (inputs != null) {
    println("Trying to trigger VeColBatch caching.")
    sparkContext.runJob(inputs.persist(StorageLevel.MEMORY_ONLY).cache(), (i: Iterator[_]) => ())
    println("Finished collect()")
  }


  def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U] = {
    val newFunc = CppTranspiler.transpileMap(expr)
    new MappedVeRDD(this, newFunc)
  }

  def veflatMap[U: ClassTag](expr: universe.Expr[T => TraversableOnce[U]]): VeRDD[U] = ???

  def vefilter(expr: Expr[T => Boolean]): VeRDD[T] = {
    val newFunc = CppTranspiler.transpileFilter(expr)

    new FilteredVeRDD[T](this, newFunc)
  }

  def vereduce(expr: Expr[(T, T) => T]): T = {
    val newFunc = CppTranspiler.transpileReduce(expr)

    val reduceResults = inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      newFunc.evalFunctionOnBatch(batches)
    }

    val ret = reduceResults.mapPartitions { batches =>
      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val klass = tag.runtimeClass
      val r = batches.map { veColBatch =>
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
        array
      }
      r
    }

    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]
    ret.asInstanceOf[RDD[Array[T]]].collect().flatten.reduce(f)
  }

  override def vegroupBy[K: Ordering: ClassTag](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    val mapped = new VeGroupByRDD(this, newFunc)
    val out = new ShuffledRDD[K, VeColBatch, VeColBatch](mapped, new HashPartitioner(this.partitions.length))
    out.setSerializer(new VeSerializer(sparkContext.getConf, true))
    new VeConcatGroups(out)
  }

  override def toRDD : RDD[T] = {
    inputs.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
      val klass = tag.runtimeClass

      batches.flatMap { veColBatch =>
        val arrowBatch = veColBatch.toArrowColumnarBatch()
        val array = if (klass == classOf[Int]) {
          arrowBatch.column(0).getInts(0, arrowBatch.numRows())
        } else if (klass == classOf[Long]) {
          arrowBatch.column(0).getLongs(0, arrowBatch.numRows())
        } else if (klass == classOf[Instant]) {
          arrowBatch.column(0).getLongs(0, arrowBatch.numRows()).map(ExtendedInstant.fromFrovedisDateTime)
        } else if (klass == classOf[Float]) {
          arrowBatch.column(0).getFloats(0, arrowBatch.numRows())
        } else {
          arrowBatch.column(0).getDoubles(0, arrowBatch.numRows())
        }
        array.toSeq.asInstanceOf[Seq[T]]
      }
    }
  }

  override protected def getPartitions: Array[Partition] = rdd.partitions
}

