package com.nec.ve

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector}
import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}

import java.nio.file.Paths
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._

class FilteredVeRDD[T: ClassTag](rdd: VeRDD[T], func: CFunction2, soPath: String, outputs: List[CVector]) extends VeRDD[T](rdd) {
  import com.nec.ve.FilteredVeRDD._

  override def reduce(f: (T, T) => T): T = macro vereduce_impl[T]

  def vereduce(expr: Expr[(T, T) => T])(implicit tag: WeakTypeTag[T]): T = {
    val start1 = System.nanoTime()

    println("filter mapPartitions")
    val mappedResults = inputs.mapPartitions { batches =>
      println(s"loading3f-1: $soPath")
      val libRef = veProcess.loadLibrary(Paths.get(soPath))

      //val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }
    }

    val end1 = System.nanoTime()

    println(s"filter evalFunction took ${(end1 - start1) / 1000000000.0}s")

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


  def vefilter(expr: Expr[T => Boolean])(implicit tag: WeakTypeTag[T]): VeRDD[T] = {
    val start1 = System.nanoTime()

    println("mapPartitions")
    val mappedResults = inputs.mapPartitions { batches =>
      println(s"loading2-1: $soPath")
      val libRef = veProcess.loadLibrary(Paths.get(soPath))

      //val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }
    }

    val end1 = System.nanoTime()

    println(s"parent evalFunction took ${(end1 - start1) / 1000000000.0}s")

    val start2 = System.nanoTime()

    // Reduce
    println("vefilter got expr: " + showRaw(expr.tree))

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

    val reduceResults = mappedResults.mapPartitions { batches =>
      println(s"loading2-2: $reduceSoPath")
      val newLibRef = veProcess.loadLibrary(Paths.get(reduceSoPath))

      batches.map { batch =>
        evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
      }
    }

    val end2 = System.nanoTime()

    println(s"filter evalFunction took ${(end2 - start2) / 1000000000.0}s")

    new FilteredVeRDD[T](this, newFunc, reduceSoPath, newOutputs)
  }

  override def filter(f: (T) => Boolean): VeRDD[T] = macro vefilter_impl[T]

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

    val out2 = results.mapPartitions { batches =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val r = batches.flatMap { veColBatch =>
        val arrowBatch = veColBatch.toArrowColumnarBatch()
        arrowBatch.column(0).getLongs(0, arrowBatch.numRows())
      }
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r
    }.asInstanceOf[RDD[T]]

    val end2 = System.nanoTime()

    println(s"results.map took ${(end2 - start2) / 1000000000.0}s")

    out2.reduce(f)
  }
}

object FilteredVeRDD {
  def vereduce_impl[T](c: whitebox.Context)(f: c.Expr[(T, T) => T]): c.Expr[T] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vereduce(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[T](x)
  }

  def vefilter_impl[T](c: whitebox.Context)(f: c.Expr[(T) => Boolean]): c.Expr[VeRDD[T]] = {
    import c.universe._

    val self = c.prefix
    val x = q"${self}.vefilter(scala.reflect.runtime.universe.reify { ${f} })"
    c.Expr[VeRDD[T]](x)
  }
}
