package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.colvector.VeColVector
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}
import org.bytedeco.javacpp.IntPointer

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class VectorizedRDD[T: ClassTag](prev: RDD[T]) extends VeRDD[T](prev) {

  val inputs: RDD[VeColBatch] = prev.mapPartitions { valsIter =>
    println("Reading inputs")
    val start = System.nanoTime()
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    val vals = valsIter.toArray.asInstanceOf[Array[T]]
    val len = vals.length

    val intVec = new IntPointer(len.asInstanceOf[Long])
    intVec.put(vals, 0, vals.length)

    /*val intVec = new IntVector("foo", new RootAllocator(Int.MaxValue))
    intVec.allocateNew()
    intVec.setValueCount(vals.length)
    vals.zipWithIndex.foreach { case (v, i) =>
      intVec.set(i, v.asInstanceOf[Int])
    }
    */
    val end = System.nanoTime()
    println(s"Took ${(end - start) / 1000000000}s to convert ${vals.length} rows.")
    Iterator(VeColBatch.fromList(List(VeColVector.fromPointer(intVec))))
    //Iterator(VeColBatch.fromList(List(VeColVector.fromArrowVector(intVec))))
  }.persist(StorageLevel.MEMORY_ONLY)
  val inputCount: Long = inputs.count()

  @Override
  def getVectorData(): RDD[VeColBatch] = inputs

}


abstract class VeRDD[T: ClassTag](prev: VeRDD[T]) extends RDD[T](prev) {
  @transient val transpiler: CppTranspiler.type = CppTranspiler


  // subclasses need to implement this
  // this (along with other operations) is where calculations on the VE are triggered
  def getVectorData(): RDD[VeColBatch]


  // first step of every vectorized
  def vectorize() = new VectorizedRDD(this)

  def vemap[U:ClassTag](expr: Expr[T => T]): MappedVeRDD[T] = {
    new MappedVeRDD(this, expr)
  }

  /*
  def vemap[U:ClassTag](expr: Expr[T => T]): MappedVeRDD = {
    import scala.reflect.runtime.universe._

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
        PointerPointer(CVector("a_in", CFunctionGeneration.VeScalarType.veNullableInt)),
        PointerPointer(outputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    // TODO: remove dummy result
    new MappedVeRDD(this.asInstanceOf[RDD[Int]], func, compiledPath.toAbsolutePath.toString, outputs)
  }


   */



  override def collect(): Array[T] = super.collect()

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    prev.compute(split, context)
  }

  case class VePartition(idx: Int) extends Partition {
    override def index: Int = idx
  }

  override protected def getPartitions: Array[Partition] = {
    //(0 until 8).map(i => VePartition(i)).toArray
    prev.partitions
  }

  def withCompiled[U](cCode: String)(f: Path => U): U = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
        .compile_c(cCode)
    f(oPath)
  }

  def evalFunction(func: CFunction2, libRef: LibraryReference, inputs: List[VeColVector], outVectors: List[CVector])(implicit ctx: OriginalCallingContext): VeColBatch = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

    VeColBatch.fromList(veProcess.execute(libRef, func.name, inputs, outVectors))
  }

  // Operations, trigger calculation
  def vereduce[U:ClassTag](expr: Expr[(Int, Int) => Int]): Int = {

    println("vereduce got expr: " + showRaw(expr.tree))

    // transpile f to C
    val code = transpiler.transpileReduce(expr)
    val funcName = s"reduce_${Math.abs(code.hashCode())}"

    val newOutputs = List(CVector("out", CFunctionGeneration.VeScalarType.veNullableInt))
    val newFunc = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", CFunctionGeneration.VeScalarType.veNullableInt)),
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

    prev.getVectorData().mapPartitions { veColBatch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val batches = veColBatch.toList
      val r = batches.flatMap(_.cols).flatMap { veColVector =>
        val intVec = veColVector.toArrowVector().asInstanceOf[IntVector]
        val ids = (0 until intVec.getValueCount)
        ids.map(intVec.get).toList
      }
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r.toIterator
    }.collect().sum
  }

  override def reduce[T](f: (T, T) => T): T = {

    val start2 = System.nanoTime()

    val out2 = prev.getVectorData().mapPartitions { veColBatch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val batches = veColBatch.toList
      val r = batches.flatMap(_.cols).flatMap { veColVector =>
        val intVec = veColVector.toArrowVector().asInstanceOf[IntVector]
        val ids = (0 until intVec.getValueCount)
        ids.map(intVec.get).toList
      }
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r.toIterator
    }.reduce(f)

    val end2 = System.nanoTime()

    println(s"results.map took ${(end2 - start2) / 1000000000.0}s")

    out2
  }
}

// implicit conversion
object VeRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VeRDD[T] = new VectorizedRDD[T](r)
}