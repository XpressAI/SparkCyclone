package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeString}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{VeNullableDouble, VeNullableFloat, VeNullableInt, VeNullableLong, VeNullableShort}
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.colvector.VeColVector
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, BigIntVector, Float8Vector, IntVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}
import org.bytedeco.javacpp.{IntPointer, Pointer}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

class VectorizedRDD[T: ClassTag](prev: RDD[T]) extends VeRDD[T](prev) {

  val inputs: RDD[VeColBatch] = prev.mapPartitions { valsIter =>
    println("Reading inputs")
    val start = System.nanoTime()
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    val vals = valsIter.toArray

    var vec = createVector(vals)
    vec.allocateNew()
    vec.setValueCount(vals.length)


    val len = vals.length

    val result = Iterator(VeColBatch.fromList(List(VeColVector.fromPointer(vec.asInstanceOf[Pointer]))))

    /*val intVec = new IntVector("foo", new RootAllocator(Int.MaxValue))
    intVec.allocateNew()
    intVec.setValueCount(vals.length)
    vals.zipWithIndex.foreach { case (v, i) =>
      intVec.set(i, v.asInstanceOf[Int])
    }
    */

    val end = System.nanoTime()
    println(s"Took ${(end - start) / 1000000000}s to convert ${vals.length} rows.")
    result
    // Iterator(VeColBatch.fromList(List(VeColVector.fromPointer(intVec))))

    //Iterator(VeColBatch.fromList(List(VeColVector.fromArrowVector(intVec))))
  }.persist(StorageLevel.MEMORY_ONLY)
  val inputCount: Long = inputs.count()

  @Override
  def getVectorData(): RDD[VeColBatch] = inputs

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


  override def reduce(f: (T, T) => T): T = {

    val out = prev.getVectorData().mapPartitions { veColBatchIt =>

      veColBatchIt.flatMap(_.cols).flatMap { veColVector =>
          veColVectorToList[T](veColVector).iterator
      }

    }.reduce(f)

    out
  }

  // convert a VeColVector to Seq[T] for the according type
  private def veColVectorToList[U](veColVector: VeColVector): Seq[U] = veColVector.veType match {
    case VeNullableInt => veColVectorToIntList(veColVector).asInstanceOf[Seq[U]]
    case VeNullableLong => veColVectorToLongList(veColVector).asInstanceOf[Seq[U]]
    case VeNullableDouble => veColVectorToDoubleList(veColVector).asInstanceOf[Seq[U]]
    // TODO:
    //case VeNullableFloat
    //case VeNullableShort
    //case VeString
  }

  private def veColVectorToIntList(veColVector: VeColVector): Seq[Int] = {
    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
    val vec = veColVector.toArrowVector().asInstanceOf[IntVector]
    (0 until vec.getValueCount).map(vec.get)
  }

  private def veColVectorToLongList(veColVector: VeColVector): Seq[Long] = {
    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
    val vec = veColVector.toArrowVector().asInstanceOf[BigIntVector]
    (0 until vec.getValueCount).map(vec.get)
  }

  private def veColVectorToDoubleList(veColVector: VeColVector): Seq[Double] = {
    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)
    val vec = veColVector.toArrowVector().asInstanceOf[Float8Vector]
    (0 until vec.getValueCount).map(vec.get)
  }

}

// implicit conversion
object VeRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VeRDD[T] = new VectorizedRDD[T](r)
}