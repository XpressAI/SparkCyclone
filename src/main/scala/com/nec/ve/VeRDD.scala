package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.colvector.VeColVector
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, BigIntVector, Float8Vector, IntVector, ValueVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.{Partition, TaskContext}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

class VeRDD[T: ClassTag](rdd: RDD[T]) extends RDD[T](rdd) {
  @transient val transpiler: CppTranspiler.type = CppTranspiler

  val inputs: RDD[VeColBatch] = rdd.mapPartitions { valsIter =>
    println("Reading inputs")
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    val vals = valsIter.toSeq

    var vec = createVector(vals)

    vec.allocateNew()
    vec.setValueCount(vals.length)
    vals.zipWithIndex.foreach { case (v, i) =>
      setValue(vec, i, v)
    }
    Iterator(VeColBatch.fromList(List(VeColVector.fromArrowVector(vec))))
  }.cache()



  private def createVector[T: ClassTag](seq: Seq[T]): BaseFixedWidthVector = seq match {
    case s: Seq[Int @unchecked] if classTag[T] == classTag[Int] => new IntVector("Int", new RootAllocator(Long.MaxValue))
    case s: Seq[Double @unchecked] if classTag[T] == classTag[Double]  => new Float8Vector("Double", new RootAllocator(Long.MaxValue))
    case s: Seq[Long @unchecked] if classTag[T] == classTag[Long] => new BigIntVector("Long", new RootAllocator(Long.MaxValue))
    // TODO: More types
  }

  private def setValue[T: ClassTag](vec: BaseFixedWidthVector, index: Int, value: T) = value match {
    case v: Int => vec.asInstanceOf[IntVector].set(index, v)
    case v: Double => vec.asInstanceOf[Float8Vector].set(index, v)
    case v: Long => vec.asInstanceOf[BigIntVector].set(index, v)
    // TODO: More types
  }

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

  override def collect(): Array[T] = super.collect()

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    rdd.compute(split, context)
  }

  case class VePartition(idx: Int) extends Partition {
    override def index: Int = idx
  }

  override protected def getPartitions: Array[Partition] = {
    //(0 until 8).map(i => VePartition(i)).toArray
    rdd.partitions
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
}

// implicit conversion
object VeRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VeRDD[T] = new VeRDD(r)
}