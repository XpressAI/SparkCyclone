package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.{SparkCycloneDriverPlugin, SparkCycloneExecutorPlugin}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.bytedeco.javacpp.LongPointer

import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
/*
class MappedVeRDD[T](rdd: VeRDD[T], func: CFunction2, soPath: String, outputs: List[CVector]) extends VeRDD[T](rdd) {
  def vereduce[U:ClassTag](expr: Expr[(T, T) => T]): T = {

    println("vereduce got expr: " + showRaw(expr.tree))

    // transpile f to C
    val code = transpiler.transpileReduce(expr)
    val funcName = s"reduce_${Math.abs(code.hashCode())}"

    val newOutputs = List(CVector("out", CFunctionGeneration.VeScalarType.veNullableLong))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", CFunctionGeneration.VeScalarType.veNullableLong)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${newFunc.toCodeLinesWithHeaders.cCode}")

    // compile
    val newCompiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(newFunc.toCodeLinesWithHeaders).toAbsolutePath.toString
    println("compiled path:" + newCompiledPath)

    val start1 = System.nanoTime()

    val libRef = veProcess.loadLibrary(Paths.get(soPath))

    val results = inputs.mapPartitions { (_) =>
      val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      Iterator(evalFunction(func, libRef, batch.cols, outputs))
    }

    val newLibRef = veProcess.loadLibrary(Paths.get(newCompiledPath))

    val results2 = results.map { batch =>
      evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
    }

    val end1 = System.nanoTime()

    println(s"reduce evalFunction took ${(end1 - start1) / 1000000000.0}s")

    val start2 = System.nanoTime()

    val ret = results2.flatMap { batch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val intVecs = batch.cols.flatMap { col =>
        val intVec = col.toArrowVector().asInstanceOf[BigIntVector]
        val ids = (0 until intVec.getValueCount)
        ids.map(intVec.get)
      }

      val end4 = System.nanoTime()

      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")

      intVecs
    }

    val end2 = System.nanoTime()

    println(s"collect().sum took ${(end2 - start2) / 1000000000.0}s")

    ret.reduce((a, b) => a + b)
  }

  override def reduce(f: (Long, Long) => Long): Long = {

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
    }

    val end2 = System.nanoTime()

    println(s"results.map took ${(end2 - start2) / 1000000000.0}s")

    out2.reduce(f)
  }
}
*/