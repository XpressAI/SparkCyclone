package com.nec.ve

import com.nec.native.{CompiledVeFunction, CppTranspiler}
import com.nec.spark.agile.merge.MergeFunction
import com.nec.util.DateTimeOps._
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

  def vegroupBy_impl[K, T](c: whitebox.Context)(f: c.Expr[T => K]): c.Expr[VeRDD[(K, Iterable[T])]] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vegroupBy(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[(K, Iterable[T])]](x)
  }

  def vesortBy_impl[K, T](c: whitebox.Context)(f: c.Expr[T => K]): c.Expr[VeRDD[(T)]] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vesortBy(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[T]](x)
  }
}

trait VeRDD[T] extends RDD[T] {

  @transient protected val toolbox: ToolBox[universe.type] = currentMirror.mkToolBox()

  implicit val tag: ClassTag[T]

  val inputs: RDD[VeColBatch]

  //def map[U](f: T => U): RDD[U] = macro vemap_impl[U, T]

  //override def reduce(f: (T, T) => T): T = macro vereduce_impl[T]

  //override def filter(f: T => Boolean): VeRDD[T] = macro vefilter_impl[T]

  //def groupBy[K](f: T => K): VeRDD[(K, Iterable[T])] = macro vegroupBy_impl[K, T]

  //def sortBy[K](f: T => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] =  macro vesortBy_impl[K]

  def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U]

  def vefilter(expr: Expr[T => Boolean]): VeRDD[T]

  def vegroupBy[K](expr: Expr[T => K]): VeRDD[(K, Iterable[T])]

  def vereduce(expr: Expr[(T, T) => T]): T

  def vesortBy[K](expr: Expr[T => K], ascending: Boolean = true, numPartitions: Int = this.partitions.length): VeRDD[T]

  def toRDD: RDD[T]

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val batches = inputs.iterator(split, context)

    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
    val klass = implicitly[ClassTag[T]].runtimeClass

    batches.flatMap { veColBatch =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

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

      //veColBatch.free()
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
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
      val res = func.evalFunctionOnBatch(batches)
      res
    }
  }

  override def toRDD : RDD[T] = {
    inputs.mapPartitions { batches =>
      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
      val klass = tag.runtimeClass
      batches.flatMap { veColBatch =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

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

        //veColBatch.free()
        array.toSeq.asInstanceOf[Seq[T]]
      }
    }
  }


  def vereduce(expr: Expr[(T, T) => T]): T = {
    val mappedResults = inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
      func.evalFunctionOnBatch(batches)
    }
    val klass = tag.runtimeClass

    val newFunc = CppTranspiler.transpileReduce(expr)

    val reduceResults = mappedResults.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      newFunc.evalFunctionOnBatch(batches)
    }

    val ret = reduceResults.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

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
    val newFunc = CppTranspiler.transpileFilter(expr)
    new FilteredVeRDD[T](this, newFunc)
  }

  override def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U] = {
    val newFunc = CppTranspiler.transpileMap(expr)
    new MappedVeRDD(this, newFunc)
  }

  override def vegroupBy[K](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    implicit val g: ClassTag[K] = newFunc.types.output.tag.asInstanceOf[ClassTag[K]]


    val mapped = new VeGroupByRDD(this, newFunc)
    val out = new ShuffledRDD[K, VeColBatch, VeColBatch](mapped, new HashPartitioner(this.partitions.length))
    out.setSerializer(new VeSerializer(sparkContext.getConf, true))
    new VeConcatGroups(out)
  }

  override def vesortBy[K](expr: Expr[T => K], ascending: Boolean = true, numPartitions: Int = this.partitions.length): VeRDD[T] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    implicit val g: ClassTag[K] = newFunc.types.output.tag.asInstanceOf[ClassTag[K]]

    val mapped = new VeGroupByRDD(this, newFunc)
    val shuffle = new ShuffledRDD[K, VeColBatch, VeColBatch](
      mapped,
      new RangePartitioner(this.partitions.length, mapped, ascending)
    ).setKeyOrdering(ord)
    shuffle.setSerializer(new VeSerializer(sparkContext.getConf, true))
    val values = shuffle.map(_._2)

    import com.nec.util.SyntaxTreeOps._

    val dataType = newFunc.types.output.tpe.toVeType

    val funcName = s"merge_${dataType.toString}"
    val code = MergeFunction(funcName, List(dataType))
    val func = CompiledVeFunction(
      code.toCFunction,
      code.toVeFunction.namedResults,
      newFunc.types.copy(input = newFunc.types.output)
    )

    new MappedVeRDD[T, VeColBatch](new RawVeRDD[T](values), func)
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
    sparkContext.runJob(inputs.persist(StorageLevel.MEMORY_ONLY).cache(), (i: Iterator[_]) => ())
  }


  def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U] = {
    val newFunc = CppTranspiler.transpileMap(expr)
    new MappedVeRDD(this, newFunc)
  }

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
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
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

  override def vegroupBy[K](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    implicit val g: ClassTag[K] = newFunc.types.output.tag.asInstanceOf[ClassTag[K]]

    val mapped = new VeGroupByRDD(this, newFunc)
    val out = new ShuffledRDD[K, VeColBatch, VeColBatch](mapped, new HashPartitioner(this.partitions.length))
    out.setSerializer(new VeSerializer(sparkContext.getConf, true))
    new VeConcatGroups(out)
  }

  override def vesortBy[K](expr: Expr[T => K], ascending: Boolean = true, numPartitions: Int = this.partitions.length): VeRDD[T] = {
    ???
  }


  override def toRDD : RDD[T] = {
    inputs.mapPartitions { batches =>
      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
      val klass = tag.runtimeClass

      batches.flatMap { veColBatch =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

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

        //veColBatch.free()
        array.toSeq.asInstanceOf[Seq[T]]
      }
    }
  }

  override protected def getPartitions: Array[Partition] = rdd.partitions
}

class RawVeRDD[T](
  rdd: RDD[VeColBatch]
)(implicit val tag: ClassTag[VeColBatch])  extends RDD[VeColBatch](rdd.sparkContext, List(new OneToOneDependency(rdd))) with VeRDD[VeColBatch] {
  override val inputs: RDD[VeColBatch] = rdd

  override def vemap[U: ClassTag](expr: universe.Expr[VeColBatch => U]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[VeColBatch => Boolean]): VeRDD[VeColBatch] = ???

  override def vegroupBy[K](expr: universe.Expr[VeColBatch => K]): VeRDD[(K, Iterable[VeColBatch])] = ???

  override def vereduce(expr: universe.Expr[(VeColBatch, VeColBatch) => VeColBatch]): VeColBatch = ???

  override def vesortBy[K](expr: universe.Expr[VeColBatch => K], ascending: Boolean, numPartitions: Int): VeRDD[VeColBatch] = ???

  override def toRDD: RDD[VeColBatch] = rdd
}