package com.nec.ve

import com.nec.arrow.colvector.ArrowVectorConversions.BPCVToFieldVector
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.spark.{SparkCycloneDriverPlugin, SparkCycloneExecutorPlugin}
import com.nec.ve.MappedVeRDD.vereduce_impl
import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, IntVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.bytedeco.javacpp.LongPointer

import java.nio.file.Paths
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._

class MappedVeRDD[T: ClassTag](rdd: VeRDD[T], func: CFunction2, soPath: String, outputs: List[CVector]) extends VeRDD[T](rdd) {
  def vereduce(expr: Expr[(T, T) => T])(implicit tag: WeakTypeTag[T]): T = {
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
    val newCompiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("wee2")
    println("compiled path:" + newCompiledPath)

    val start1 = System.nanoTime()

    println("loading2")
    val libRef = veProcess.loadLibrary(Paths.get(soPath))

    println("mapPartitions")
    val results = inputs.mapPartitions { (_) =>
      val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      Iterator(evalFunction(func, libRef, batch.cols, outputs))
    }

    println("loading2")
    val newLibRef = veProcess.loadLibrary(Paths.get(newCompiledPath))

    val results2 = results.mapPartitions { batches =>
      batches.map { batch =>
        evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
      }
    }

    val end1 = System.nanoTime()

    println(s"reduce evalFunction took ${(end1 - start1) / 1000000000.0}s")

    val start2 = System.nanoTime()

    val ret = results2.flatMap { batch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val intVecs = batch.cols.map { col =>
        col.toBytePointerVector().toArrowVector(allocator)
      }

      val end4 = System.nanoTime()

      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")

      intVecs
    }

    val end2 = System.nanoTime()

    println(s"collect().sum took ${(end2 - start2) / 1000000000.0}s")
    import scala.reflect.runtime.currentMirror
    import scala.reflect.runtime.universe._
    import scala.tools.reflect.ToolBox
    val toolbox = currentMirror.mkToolBox()
    val f = toolbox.eval(expr.tree).asInstanceOf[(T, T) => T]

    val ret2 = ret.flatMap { (v: FieldVector) =>
      tag.tpe match {
        case t: Type if t =:= typeOf[Long] || t =:= typeOf[Int] =>
          v match {
            case vec: BigIntVector =>
              (0 until vec.getValueCount).map(i => vec.get(i))
            case vec: IntVector =>
              (0 until vec.getValueCount).map(i => vec.get(i))
          }
        case t: Type if t =:= typeOf[Option[Long]] || t =:= typeOf[Option[Int]] =>
          v match {
            case vec: BigIntVector =>
              (0 until vec.getValueCount).map(i => if (vec.isNull(i)) None else Some(vec.get(i)))
            case vec: IntVector =>
              (0 until vec.getValueCount).map(i => if (vec.isNull(i)) None else Some(vec.get(i)))
          }
      }
    }.asInstanceOf[RDD[T]].reduce(f)

    ret2
  }

  //override def filter(f: (T) => Boolean): RDD[T] = macro vefilter_impl[T]

  override def reduce(f: (T, T) => T): T = macro vereduce_impl[T]

  def reduceCpu(f: (T, T) => T): T = {

    val start1 = System.nanoTime()

    val results = rdd.inputs.mapPartitions { inputIterator =>
      //val start3 = System.nanoTime()


      val libRef = veProcess.loadLibrary(Paths.get(soPath))
      val batches = inputIterator.toList
      val iter = batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }
      //val end3 = System.nanoTime()
      //println(s"reducing... took ${(end3 - start3) / 1000000000.0}s ")
      iter.toIterator
    }

    val end1 = System.nanoTime()

    println(s"evalFunction took ${(end1 - start1) / 1000000000.0}s")

    val start2 = System.nanoTime()

    val out2 = results.mapPartitions { veColBatch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val batches = veColBatch.toList
      val r = batches.flatMap(_.cols).flatMap { veColVector =>
        val intVec = veColVector.toBytePointerVector().underlying.containerLocation.get.asInstanceOf[LongPointer]
        val a: Array[Long] = Array[Long](intVec.limit().toInt)
        intVec.get(a, 0, intVec.limit().toInt)
        a
        //val intVec = veColVector.toArrowVector().asInstanceOf[BigIntVector]
        //val ids = (0 until intVec.getValueCount)
        //ids.map(intVec.get).toList
      }
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r.toIterator
    }.asInstanceOf[RDD[T]]

    val end2 = System.nanoTime()

    println(s"results.map took ${(end2 - start2) / 1000000000.0}s")

    out2.reduce(f)
  }
}

object MappedVeRDD {
  def vereduce_impl[T](c: whitebox.Context)(f: c.Expr[(T, T) => T]): c.Expr[T] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vereduce(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[T](x)
  }

  def vefilter_impl[T](c: whitebox.Context)(f: c.Expr[(T) => Boolean]): c.Expr[RDD[T]] = {
    import c.universe._

    val self = c.prefix
    val x = q"new FilteredVeRDD(${self}, scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[RDD[T]](x)
  }
}