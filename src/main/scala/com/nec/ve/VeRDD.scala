package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.spark.{SparkCycloneDriverPlugin, SparkCycloneExecutorPlugin}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.VeRDD.vemap_impl
import com.nec.ve.colvector.VeColVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._

trait VeRDD[T] extends RDD[T] {
  @transient val transpiler: CppTranspiler.type = CppTranspiler

  val inputs: RDD[VeColBatch]

  def vemap[U: ClassTag](expr: Expr[T => U]): MappedVeRDD[U] = {
    import scala.reflect.runtime.universe._

    // TODO: for inspection, remove when done
    println("vemap got expr: " + showRaw(expr.tree))

    // TODO: Get AST (Expr) from symbol table, when necessary
    // val expr = ...

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
    println("wee1")
    println("compiled path:" + compiledPath)

    new MappedVeRDD(this.asInstanceOf[RDD[U]], func, compiledPath.toAbsolutePath.toString, outputs)
  }

  def withCompiled[U](cCode: String)(f: Path => U): U = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
      .compile_c(cCode)
    f(oPath)
  }


  def evalFunction(
    func: CFunction2,
    libRef: LibraryReference,
    inputs: List[VeColVector],
    outVectors: List[CVector]
  )(implicit ctx: OriginalCallingContext): VeColBatch = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

    VeColBatch.fromList(veProcess.execute(libRef, func.name, inputs, outVectors))
  }
}

class BasicVeRDD[T: ClassTag](
  rdd: RDD[T]
)(implicit tag: WeakTypeTag[T]) extends RDD[T](rdd.sparkContext, List(new OneToOneDependency(rdd))) with VeRDD[T] {
  val inputs: RDD[VeColBatch] = rdd.mapPartitionsWithIndex { case (index, valsIter) =>
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    if (SparkCycloneExecutorPlugin.containsCachedBatch("inputs")) {
      println(s"Using cached inputs for ${index}")
      Iterator(SparkCycloneExecutorPlugin.getCachedBatch(s"inputs"))
    } else {
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
      SparkCycloneExecutorPlugin.registerCachedBatch("inputs", batch)
      Iterator(batch)
    }
  }

  // Trigger caching of VeColBatches
  println("Trying to trigger VeColBatch caching.")
  inputs.count()
  println("Finished collect()")

  def map(f: T => T): MappedVeRDD[T] = macro vemap_impl[T]

  override def compute(split: Partition, context: TaskContext): Iterator[T] = rdd.compute(split, context)

  override protected def getPartitions: Array[Partition] = rdd.partitions
}

// implicit conversion
object VeRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VeRDD[T] = new BasicVeRDD(r)

  def vemap_impl[T](c: whitebox.Context)(f: c.Expr[T => T]): c.Expr[MappedVeRDD[T]] = {
    import c.universe._
    val self = c.prefix
    val x = q"${self}.vemap(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[MappedVeRDD[T]](x)
  }
}
