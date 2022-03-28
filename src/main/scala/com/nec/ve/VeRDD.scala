package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.colvector.VeColVector
import org.apache.arrow.memory.RootAllocator
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.storage.StorageLevel

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.collection.immutable.NumericRange
import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

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

  def vegroupBy_impl[K, T](c: whitebox.Context)(f: c.Expr[T => K]): c.Expr[VeRDD[(K, Iterable[T])]] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vegroupBy(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[(K, Iterable[T])]](x)
  }
}

class KeyedVeRDD[K: Ordering : ClassTag, V: ClassTag, P <: Product2[K, V] : ClassTag](self: VeRDD[P]) extends Serializable {
  private val ordering = implicitly[Ordering[K]]

  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = {
    // TODO: SHM shuffle
    new ShuffledVeRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }
}

trait VeRDD[T] extends RDD[T] {
  import VeRDD._
  @transient val transpiler: CppTranspiler.type = CppTranspiler
  implicit val tag: WeakTypeTag[T]

  val inputs: RDD[VeColBatch]

  def map[U](f: T => U): RDD[U] = macro vemap_impl[U, T]
  def flatMap[U](f: T => TraversableOnce[U]): VeRDD[U] = macro veflatMap_impl[U, T]
  override def reduce(f: (T, T) => T): T = macro vereduce_impl[T]
  override def filter(f: T => Boolean): VeRDD[T] = macro vefilter_impl[T]
  def groupBy[K](f: T => K): VeRDD[(K, Iterable[T])] = macro vegroupBy_impl[K, T]

  def withCompiled[U](cCode: String)(f: Path => U): U = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
      .compile_c(cCode)
    f(oPath)
  }

  def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U]
  def veflatMap[U: ClassTag](expr: Expr[T => TraversableOnce[U]]): VeRDD[U]
  def vefilter(expr: Expr[T => Boolean])(implicit tag: WeakTypeTag[T]): VeRDD[T]
  def vegroupBy[K](expr: Expr[T => K]): VeRDD[(K, Iterable[T])]
  def vereduce(expr: Expr[(T, T) => T])(implicit tag: WeakTypeTag[T]): T


  def evalFunction(
    func: CFunction2,
    libRef: LibraryReference,
    inputs: List[VeColVector],
    outVectors: List[CVector]
  )(implicit ctx: OriginalCallingContext): VeColBatch = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

    VeColBatch.fromList(veProcess.execute(libRef, func.name, inputs, outVectors))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    val batches = inputs.iterator(split, context)

    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
    val klass = tag.tpe

    //val klass = implicitly[ClassTag[T]].runtimeClass

    batches.flatMap { veColBatch =>
      val arrowBatch = veColBatch.toArrowColumnarBatch()
      val array = if (klass =:= typeOf[Int]) {
        arrowBatch.column(0).getInts(0, arrowBatch.numRows())
      } else if (klass =:= typeOf[Long]) {
        arrowBatch.column(0).getLongs(0, arrowBatch.numRows())
      } else if (klass =:= typeOf[Float]) {
        arrowBatch.column(0).getFloats(0, arrowBatch.numRows())
      } else {
        arrowBatch.column(0).getDoubles(0, arrowBatch.numRows())
      }
      array.toSeq.asInstanceOf[Seq[T]]
    }
  }
  override protected def getPartitions: Array[Partition] = inputs.partitions

}

abstract class ChainedVeRDD[T: ClassTag](
  verdd: VeRDD[_],
  func: CFunction2,
  soPath: String,
  outputs: List[CVector]
)(implicit val tag: WeakTypeTag[T]) extends RDD[T](verdd) with VeRDD[T] {
  override val inputs: RDD[VeColBatch] = computeVe()

  def computeVe(): RDD[VeColBatch] = {
    verdd.inputs.mapPartitions { batches =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val libRef = veProcess.loadLibrary(Paths.get(soPath))

      //val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }
    }
  }




  override def vereduce(expr: Expr[(T, T) => T])(implicit tag: WeakTypeTag[T]): T = {
    val start1 = System.nanoTime()

    println("mapPartitions")
    val mappedResults = inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      println(s"loading2-1: $soPath")
      val libRef = veProcess.loadLibrary(Paths.get(soPath))

      //val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }
    }

    val end1 = System.nanoTime()

    println(s"map evalFunction took ${(end1 - start1) / 1000000000.0}s")

    val start2 = System.nanoTime()

    // Reduce
    println("vereduce got expr: " + showRaw(expr.tree))

    val klass = implicitly[ClassTag[T]].runtimeClass

    // transpile f to C
    val code = transpiler.transpileReduce(expr, klass)
    val funcName = s"reduce_${Math.abs(code.hashCode())}"

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val newOutputs = List(CVector("out", dataType))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val reduceSoPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + reduceSoPath)

    val reduceResults = mappedResults.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      println(s"loading2-2: $reduceSoPath")
      val newLibRef = veProcess.loadLibrary(Paths.get(reduceSoPath))

      batches.map { batch =>
        evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
      }
    }

    val end2 = System.nanoTime()

    println(s"reduce evalFunction took ${(end2 - start2) / 1000000000.0}s")


    val start3 = System.nanoTime()

    val ret = reduceResults.mapPartitions { batches =>
      val start4 = System.nanoTime()

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
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r
    }

    val end3 = System.nanoTime()

    println(s"toArrowVector took ${(end3 - start3) / 1000000000.0}s")

    val start4 = System.nanoTime()

    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    val toolbox = currentMirror.mkToolBox()
    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]

    val finalReduce = ret.asInstanceOf[RDD[Array[T]]].collect().flatten.reduce(f)

    val end4 = System.nanoTime()

    println(s"reducing CPU took ${(end4 - start4) / 1000000000.0} s")

    finalReduce
  }


  override def vefilter(expr: Expr[T => Boolean])(implicit tag: WeakTypeTag[T]): VeRDD[T] = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    // transpile f to C
    val code = transpiler.transpileFilter(expr, klass)
    val funcName = s"filter_${Math.abs(code.hashCode())}"

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val newOutputs = List(CVector("out", dataType))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val reduceSoPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + reduceSoPath)

    val reduceResults = inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      println(s"loading2-2: $reduceSoPath")
      val newLibRef = veProcess.loadLibrary(Paths.get(reduceSoPath))

      batches.map { batch =>
        evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
      }
    }

    val end2 = System.nanoTime()

    new FilteredVeRDD[T](this, newFunc, reduceSoPath, newOutputs)
  }

  override def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U] = {
    val klass = implicitly[ClassTag[U]].runtimeClass

    // transpile f to C
    val code = transpiler.transpile(expr, klass)
    val funcName = s"eval_${Math.abs(code.hashCode())}"


    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }


    val outputs = List(CVector("out", dataType))
    val func = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(outputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    new MappedVeRDD(this, func, compiledPath.toAbsolutePath.toString, outputs)
  }

  override def veflatMap[U: ClassTag](expr: Expr[T => TraversableOnce[U]]): VeRDD[U] = ???

  override def vegroupBy[K](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    // transpile f to C
    val code = transpiler.transpileGroupBy(expr, klass)
    val funcName = s"groupby_${Math.abs(code.hashCode())}"

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val newOutputs = List(CVector("out", dataType))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val reduceSoPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + reduceSoPath)

    new VeGroupByRDD(this, func, reduceSoPath, newOutputs)
  }
}

class BasicVeRDD[T: ClassTag](
  rdd: RDD[T]
)(implicit val tag: WeakTypeTag[T])  extends RDD[T](rdd.sparkContext, List(new OneToOneDependency(rdd))) with VeRDD[T] {
  val inputs: RDD[VeColBatch] = rdd.mapPartitionsWithIndex { case (index, valsIter) =>
    import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    println(s"Reading inputs for ${index}")
    val start = System.nanoTime()

    val valsArray = valsIter.toArray

    import com.nec.arrow.colvector.ArrayTConversions._

    val klass = implicitly[ClassTag[T]].runtimeClass
    val veVector = if (klass == classOf[Int]) {
      val intVector = valsArray.asInstanceOf[Array[Int]].toBytePointerColVector(s"inputs-${index}")
      intVector.toVeColVector()
    } else if (klass == classOf[Long]) {
      val intVector = valsArray.asInstanceOf[Array[Long]].toBytePointerColVector(s"inputs-${index}")
      intVector.toVeColVector()
    } else if (klass == classOf[Float]) {
      val intVector = valsArray.asInstanceOf[Array[Float]].toBytePointerColVector(s"inputs-${index}")
      intVector.toVeColVector()
    } else {
      val intVector = valsArray.asInstanceOf[Array[Double]].toBytePointerColVector(s"inputs-${index}")
      intVector.toVeColVector()

    }

    val end = System.nanoTime()

    println(s"Took ${(end - start) / 1000000000}s to convert ${valsArray.length} rows.")

    val batch = VeColBatch.fromList(List(veVector))
    Iterator(batch)
  }

  // Trigger caching of VeColBatches
  if (inputs != null) {
    println("Trying to trigger VeColBatch caching.")
    sparkContext.runJob(inputs.persist(StorageLevel.MEMORY_ONLY).cache(), (i: Iterator[_]) => ())
    println("Finished collect()")
  }


  override def vemap[U: ClassTag](expr: Expr[T => U]): VeRDD[U] = {
    val klass = implicitly[ClassTag[U]].runtimeClass

    // transpile f to C
    val code = transpiler.transpile(expr, klass)
    val funcName = s"eval_${Math.abs(code.hashCode())}"


    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }


    val outputs = List(CVector("out", dataType))
    val func = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(outputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    new MappedVeRDD(this, func, compiledPath.toAbsolutePath.toString, outputs)
  }

  override def veflatMap[U: ClassTag](expr: universe.Expr[T => TraversableOnce[U]]): VeRDD[U] = ???

  override def vefilter(expr: Expr[T => Boolean])(implicit tag: WeakTypeTag[T]): VeRDD[T] = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    // transpile f to C
    val code = transpiler.transpileFilter(expr, klass)
    val funcName = s"filter_${Math.abs(code.hashCode())}"

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val newOutputs = List(CVector("out", dataType))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val reduceSoPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + reduceSoPath)

    new FilteredVeRDD[T](this, newFunc, reduceSoPath, newOutputs)
  }

  override def vereduce(expr: Expr[(T, T) => T])(implicit tag: WeakTypeTag[T]): T = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    // transpile f to C
    val code = transpiler.transpileReduce(expr, klass)
    val funcName = s"reduce_${Math.abs(code.hashCode())}"

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val newOutputs = List(CVector("out", dataType))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val reduceSoPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + reduceSoPath)

    val reduceResults = inputs.mapPartitions { batches =>
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      println(s"loading2-2: $reduceSoPath")
      val newLibRef = veProcess.loadLibrary(Paths.get(reduceSoPath))

      batches.map { batch =>
        evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
      }
    }

    val ret = reduceResults.mapPartitions { batches =>
      val start4 = System.nanoTime()

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
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r
    }

    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    val toolbox = currentMirror.mkToolBox()
    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]

    val finalReduce = ret.asInstanceOf[RDD[Array[T]]].collect().flatten.reduce(f)
    finalReduce
  }

  override def vegroupBy[K](expr: Expr[T => K]): VeRDD[(K, Iterable[T])] = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    // transpile f to C
    val code = transpiler.transpileGroupBy(expr, klass)
    val funcName = s"groupby_${Math.abs(code.hashCode())}"

    val dataType = if (klass == classOf[Int]) {
      SparkExpressionToCExpression.sparkTypeToVeType(IntegerType)
    } else if (klass == classOf[Long]) {
      SparkExpressionToCExpression.sparkTypeToVeType(LongType)
    } else if (klass == classOf[Float]) {
      SparkExpressionToCExpression.sparkTypeToVeType(FloatType)
    } else {
      SparkExpressionToCExpression.sparkTypeToVeType(DoubleType)
    }

    val newOutputs = List(CVector("out", dataType))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val reduceSoPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + reduceSoPath)

    ???
  }

  override protected def getPartitions: Array[Partition] = rdd.partitions
}

