package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import org.apache.spark.rdd.RDD

import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

//class MappedVeRDD(prev: VeRDD[Int], func: CFunction2, soPath: String, outputs: List[CVector]) extends VeRDD[Int](prev) {

class MappedVeRDD[T: ClassTag](prev: VeRDD[T], expr: Expr[T => T]) extends VeRDD[T](prev) {

  @Override
  def getVectorData(): RDD[VeColBatch] = {

    val start1 = System.nanoTime()

    // TODO: calculate only as needed, cache

    // TODO: for inspection, remove when done
    println("vemap got expr: " + showRaw(expr.tree))

    // TODO: Get AST (Expr) from symbol table, when necessary
    // val expr = ...

    // transpile f to C
    val code = transpiler.transpile(expr)
    val funcName = s"eval_${Math.abs(code.hashCode())}"

    val outputs = List(CVector("out", CFunctionGeneration.VeScalarType.veNullableInt))
    val func = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", CFunctionGeneration.VeScalarType.veNullableInt)), // TODO: More types
        PointerPointer(outputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    // Call compiled
    val results = prev.getVectorData().mapPartitions { inputIterator =>

      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val libRef = veProcess.loadLibrary(compiledPath)
      val batches = inputIterator.toList
      val iter = batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }

      iter.toIterator
    }

    val end1 = System.nanoTime()

    println(s"MappedVeRDD.getVectorData took ${(end1 - start1) / 1000000000.0}s")

    results
  }
}

/*
object MappedVeRDD {
  implicit def toMappedRDD[T: ClassTag](rdd: RDD[T] with Vectorized): MappedVeRDD[T] = new MappedVeRDD[T](rdd)
}*/