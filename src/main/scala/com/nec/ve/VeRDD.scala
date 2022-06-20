package com.nec.ve

import com.nec.colvector.{VeColVector, VeColBatch}
import com.nec.native.transpiler.{CompiledVeFunction, CompilerToolBox, CppTranspiler}
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.VeType
import com.nec.util.DateTimeOps._
import com.nec.ve.serializer.VeSerializer
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}

import java.time.Instant
import scala.collection.immutable.NumericRange
import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object VeRDD {
  implicit class VeRichRDD[T: ClassTag: TypeTag](rdd: RDD[T]) {
    def toVeRDD: VeRDD[T] = new BasicVeRDD[T](rdd)
  }

  implicit class PairVeRDDFunctions[K: TypeTag, V: TypeTag](self: VeRDD[(K, V)]){
    def vejoin[W: TypeTag](other: VeRDD[(K, W)]): VeRDD[(K, V, W)] = VeJoinRDD(self, other)
  }

  implicit class VeRichSparkContext(sc: SparkContext) {
    def veParallelize(range: NumericRange.Inclusive[Long]): VeRDD[Long] = {
      SequenceVeRDD.makeSequence(sc, range.start, range.end)
    }
  }

  def vemap_impl[U, T](c: whitebox.Context)(f: c.Expr[T => U]): c.Expr[VeRDD[U]] = {
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
  import VeRDD._

  @transient protected val toolbox: ToolBox[universe.type] = CompilerToolBox.get

  implicit val typeTag: TypeTag[T]
  implicit val tag: ClassTag[T] = ClassTag(typeTag.mirror.runtimeClass(typeTag.tpe))

  val inputs: RDD[VeColBatch]

  def map[U](f: T => U): VeRDD[U] = macro vemap_impl[U, T]

  override def reduce(f: (T, T) => T): T = macro vereduce_impl[T]

  override def filter(f: T => Boolean): VeRDD[T] = macro vefilter_impl[T]

  def groupBy[K](f: T => K): VeRDD[(K, Iterable[T])] = macro vegroupBy_impl[K, T]

  //def sortBy[K](f: T => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] =  macro vesortBy_impl[K]

  def vemap[U: TypeTag](expr: Expr[T => U]): VeRDD[U]

  def vefilter(expr: Expr[T => Boolean]): VeRDD[T]

  def vegroupBy[K: TypeTag](expr: Expr[T => K]): VeRDD[(K, Iterable[T])]

  def vereduce(expr: Expr[(T, T) => T]): T

  def vesortBy[K: TypeTag](expr: Expr[T => K], ascending: Boolean = true, numPartitions: Int = this.partitions.length): VeRDD[T]

  def toRDD : RDD[T] = {
    inputs.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
     import com.nec.util.CallContextOps._

      batches.flatMap { veColBatch =>
        val res = veColBatch.toCPUSeq[T]
        veColBatch.free()
        res
      }
    }
  }


  def flatMap[U](f: T => TraversableOnce[U])(implicit uTag: TypeTag[U]): VeRDD[U] = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    implicit val cTag: ClassTag[U] =  ClassTag[U](mirror.runtimeClass(uTag.tpe))

    val rdd = toRDD
    val flatMapped = rdd.flatMap(f)

    new BasicVeRDD(flatMapped)
  }

  def veflatMap[U: TypeTag](expr: Expr[T => TraversableOnce[U]]): VeRDD[U] = {
    val f = toolbox.eval(expr.tree).asInstanceOf[T => TraversableOnce[U]]
    flatMap(f)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
   import com.nec.util.CallContextOps._

    val batches = inputs.iterator(split, context)
    batches.flatMap { veColBatch =>
      val res = veColBatch.toCPUSeq[T]
      veColBatch.free()
      res
    }
  }

  override protected def getPartitions: Array[Partition] = inputs.partitions

  def veType[U](classTag: ClassTag[U]): VeType = {
    val klass = classTag.runtimeClass
    SparkExpressionToCExpression.sparkTypeToVeType(
    if (klass == classOf[Int]) { IntegerType }
    else if (klass == classOf[Long]) { LongType }
    else if (klass == classOf[Double]) { DoubleType }
    else if (klass == classOf[Float]) { FloatType }
    else if (klass == classOf[Instant]) { LongType }
    else {
      throw new IllegalArgumentException(s"computeMergeVe klass $klass")
    })
  }

}

abstract class ChainedVeRDD[T](
  verdd: VeRDD[_],
  func: CompiledVeFunction
)(implicit val typeTag: TypeTag[T]) extends RDD[T](verdd)(ClassTag(typeTag.mirror.runtimeClass(typeTag.tpe))) with VeRDD[T] {
  override val inputs: RDD[VeColBatch] = computeVe()

  def computeVe(): RDD[VeColBatch] = {
    verdd.inputs.mapPartitions { batches =>
     import com.nec.util.CallContextOps._
      val res = func.evalFunctionOnBatch(batches)
      res
    }
  }

  def vereduce(expr: Expr[(T, T) => T]): T = {
    val newFunc = CppTranspiler.transpileReduce(expr)

    val reduceResults = inputs.mapPartitions { batches =>
     import com.nec.util.CallContextOps._
      newFunc.evalFunctionOnBatch(batches)
    }

    val ret = reduceResults.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
     import com.nec.util.CallContextOps._

      batches.map { veColBatch =>
        val res = veColBatch.toCPUSeq[T]
        veColBatch.free()
        res
      }
    }

    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]
    ret.collect().flatten.reduce(f)
  }

  override def vefilter(expr: Expr[T => Boolean]): VeRDD[T] = {
    val newFunc = CppTranspiler.transpileFilter(expr)
    new FilteredVeRDD[T](this, newFunc)
  }

  override def vemap[U: TypeTag](expr: Expr[T => U]): VeRDD[U] = {
    val newFunc = CppTranspiler.transpileMap(expr)
    new MappedVeRDD(this, newFunc)
  }

  override def vegroupBy[K: TypeTag](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    val typeK = implicitly[TypeTag[K]]
    implicit val kClassTag: ClassTag[K] = ClassTag(typeK.mirror.runtimeClass(typeK.tpe))

    val mapped = new VeGroupByRDD[K, T](this, newFunc).toRDD.map { case (idx: K, veColBatch: VeColBatch) =>
      import com.nec.spark.SparkCycloneExecutorPlugin._
      import com.nec.util.CallContextOps._

      require(
        veColBatch.nonEmpty,
        s"Expected ${veColBatch} to be non-empty (redundant transfers)"
      )
      TaskContext.get().addTaskCompletionListener[Unit](_ => veColBatch.free())

      (idx, veColBatch)
    }

    val out = new ShuffledRDD[K, VeColBatch, VeColBatch](mapped, new HashPartitioner(this.partitions.length))
    out.setSerializer(new VeSerializer(sparkContext.getConf))
    new VeConcatGroups[K, T](out)
  }

  override def vesortBy[K: TypeTag](expr: Expr[T => K], ascending: Boolean = true, numPartitions: Int = this.partitions.length): VeRDD[T] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    implicit val g: ClassTag[K] = newFunc.types.output.tag.asInstanceOf[ClassTag[K]]

    val mapped = new VeGroupByRDD[K, T](this, newFunc).toRDD.map { case (idx: K, veColBatch: VeColBatch) =>
      import com.nec.spark.SparkCycloneExecutorPlugin._
      import com.nec.util.CallContextOps._

      require(
        veColBatch.nonEmpty,
        s"Expected ${veColBatch} to be non-empty (redundant transfers)"
      )
      TaskContext.get().addTaskCompletionListener[Unit](_ => veColBatch.free())

      (idx, veColBatch)
    }
    val shuffle = new ShuffledRDD[K, VeColBatch, VeColBatch](
      mapped,
      new RangePartitioner(this.partitions.length, mapped, ascending)
    ).setKeyOrdering(ord)
    shuffle.setSerializer(new VeSerializer(sparkContext.getConf))
    val values = shuffle.map(_._2)

    VeConcatRDD[T, T](values, newFunc.types.copy(output = newFunc.types.input))
  }
}

class BasicVeRDD[T](
  rdd: RDD[T]
)(implicit val typeTag: TypeTag[T])  extends RDD[T](rdd.sparkContext, List(new OneToOneDependency(rdd)))(ClassTag(typeTag.mirror.runtimeClass(typeTag.tpe))) with VeRDD[T] {
  val inputs: RDD[VeColBatch] = rdd.mapPartitionsWithIndex { case (index, valsIter) =>
    val batch = typeTag.tpe.asInstanceOf[TypeRef].args match {
      case Nil => VeColBatch(List(convertToVeVector(valsIter, index, 0, typeTag.tpe)))
      case args =>
        val inputList = valsIter.toList
        val columns = args.zipWithIndex.map { case (tpe, idx) =>
          val column = inputList.map(el => el.asInstanceOf[Product].productElement(idx))
          convertToVeVector(column.iterator, index, idx, tpe)
        }

        VeColBatch(columns)
    }
    Iterator(batch)
  }

  // Trigger caching of VeColBatches
  //if (inputs != null) {
  //  sparkContext.runJob(inputs.persist(StorageLevel.MEMORY_ONLY).cache(), (i: Iterator[_]) => ())
  //}

  def convertToVeVector(valsIter: Iterator[_], index: Int, seriesIndex: Int, tpe: Type): VeColVector = {
    import com.nec.colvector.ArrayTConversions._
    import com.nec.spark.SparkCycloneExecutorPlugin.veMetrics
    import com.nec.spark.SparkCycloneExecutorPlugin._
   import com.nec.util.CallContextOps._

    val klass = typeTag.mirror.runtimeClass(tpe)
    val klassTag = ClassTag[Any](klass)
    val veVector = if(tpe =:= typeOf[Instant]){
      valsIter.map(_.asInstanceOf[Instant].toFrovedisDateTime).toArray.toBytePointerColVector(s"inputs-${index}-${seriesIndex}").toVeColVector
    }else{
      ArrayTToBPCV(valsIter.toArray(klassTag))(klassTag).toBytePointerColVector(s"inputs-${index}-${seriesIndex}").toVeColVector
    }
    veVector
  }

  def vemap[U: TypeTag](expr: Expr[T => U]): VeRDD[U] = {
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
     import com.nec.util.CallContextOps._

      newFunc.evalFunctionOnBatch(batches)
    }

    val ret = reduceResults.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
     import com.nec.util.CallContextOps._

      batches.map { veColBatch =>
        val res = veColBatch.toCPUSeq[T]
        veColBatch.free()
        res
      }
    }

    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]
    ret.collect().flatten.reduce(f)
  }

  override def vegroupBy[K: TypeTag](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    val typeK = implicitly[TypeTag[K]]
    implicit val kClassTag: ClassTag[K] = ClassTag(typeK.mirror.runtimeClass(typeK.tpe))

    val mapped = new VeGroupByRDD[K, T](this, newFunc).toRDD.map { case (idx: K, veColBatch: VeColBatch) =>
      import com.nec.spark.SparkCycloneExecutorPlugin._
      import com.nec.util.CallContextOps._

      require(
        veColBatch.nonEmpty,
        s"Expected ${veColBatch} to be non-empty (redundant transfers)"
      )
      TaskContext.get().addTaskCompletionListener[Unit](_ => veColBatch.free())

      (idx, veColBatch)
    }

    val out = new ShuffledRDD[K, VeColBatch, VeColBatch](mapped, new HashPartitioner(this.partitions.length))
    out.setSerializer(new VeSerializer(sparkContext.getConf))
    new VeConcatGroups[K, T](out)
  }

  override def vesortBy[K: TypeTag](expr: Expr[T => K], ascending: Boolean = true, numPartitions: Int = this.partitions.length): VeRDD[T] = {
    val newFunc = CppTranspiler.transpileGroupBy(expr)
    implicit val ord: Ordering[K] = newFunc.types.output.ordering.asInstanceOf[Ordering[K]]
    implicit val g: ClassTag[K] = newFunc.types.output.tag.asInstanceOf[ClassTag[K]]

    val mapped = new VeGroupByRDD[K, T](this, newFunc).toRDD.map { case (idx: K, veColBatch: VeColBatch) =>
      import com.nec.spark.SparkCycloneExecutorPlugin._
      import com.nec.util.CallContextOps._

      require(
        veColBatch.nonEmpty,
        s"Expected ${veColBatch} to be non-empty (redundant transfers)"
      )
      TaskContext.get().addTaskCompletionListener[Unit](_ => veColBatch.free())

      (idx, veColBatch)
    }
    val shuffle = new ShuffledRDD[K, VeColBatch, VeColBatch](
      mapped,
      new RangePartitioner(this.partitions.length, mapped, ascending)
    ).setKeyOrdering(ord)
    shuffle.setSerializer(new VeSerializer(sparkContext.getConf))
    val values = shuffle.map(_._2)

    VeConcatRDD[T, T](values, newFunc.types.copy(output = newFunc.types.input))
  }

  override protected def getPartitions: Array[Partition] = rdd.partitions
}

class RawVeRDD[T](
  rdd: RDD[VeColBatch]
)(implicit val typeTag: TypeTag[VeColBatch])  extends RDD[VeColBatch](rdd.sparkContext, List(new OneToOneDependency(rdd))) with VeRDD[VeColBatch] {
  override val inputs: RDD[VeColBatch] = rdd

  override def vemap[U: TypeTag](expr: universe.Expr[VeColBatch => U]): VeRDD[U] = ???

  override def vefilter(expr: universe.Expr[VeColBatch => Boolean]): VeRDD[VeColBatch] = ???

  override def vegroupBy[K: TypeTag](expr: universe.Expr[VeColBatch => K]): VeRDD[(K, Iterable[VeColBatch])] = ???

  override def vereduce(expr: universe.Expr[(VeColBatch, VeColBatch) => VeColBatch]): VeColBatch = ???

  override def vesortBy[K: TypeTag](expr: universe.Expr[VeColBatch => K], ascending: Boolean, numPartitions: Int): VeRDD[VeColBatch] = ???

  override def toRDD: RDD[VeColBatch] = rdd
}
