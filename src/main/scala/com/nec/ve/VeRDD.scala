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
import org.apache.commons.io.file.Counters.BigIntegerPathCounters
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}
import org.bytedeco.javacpp.{DoublePointer, IntPointer, LongPointer, Pointer}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

trait Vectorized {
  def getVectorData(): RDD[VeColBatch]
}

/**
 * An RDD that provides a vectorized copy of the data from the previous node in the DAG.
 * This is the adapter node between the "vectorized" and the "non-vecotrized" parts of
 * the RDDs in the graph. All nodes that follow the VectorizedRDD node must be of type
 * VeRDD. Only a single VectorizedRDD should be used in each subgraph of the DAG.
 *
 * @param prev The previous RDD node in the DAG
 * @param classTag$T$0 The information about the erased class of type T
 * @tparam T The type parameter of (for now scalar) data to operate on, i.e. Int, Double, Long, ...
 */
class VectorizedRDD[T: ClassTag](prev: RDD[T]) extends RDD[T](prev) with Vectorized {

  ///
  val inputs: RDD[VeColBatch] = prev.mapPartitions { valsIter =>
    println("Reading inputs")
    val start = System.nanoTime()
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    val vals = valsIter.toArray

    /*
    var vec = createVector(vals)
    vec.allocateNew()
    vec.setValueCount(vals.length)
*/

    val pointer: Pointer = createPointer(vals)

    val len = vals.length

    val result = Iterator(VeColBatch.fromList(List(VeColVector.fromPointer(pointer))))

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

  override def getVectorData(): RDD[VeColBatch] = inputs

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    prev.compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = {
    //(0 until 8).map(i => VePartition(i)).toArray
    prev.partitions
  }

  override def reduce(f: (T, T) => T): T = {

    // Special case: We own a copy of prev's data in VeColBatch format
    val out = this.getVectorData().mapPartitions { veColBatchIt =>

      veColBatchIt.flatMap(_.cols).flatMap { veColVector =>
        veColVectorToList[T](veColVector).iterator
      }

    }.reduce(f)

    out
  }

  private def arrowToPointer[T](vector: T): Pointer = vector match {
    case iv: IntVector => iv.asInstanceOf[IntPointer]
    case biv: BigIntVector => biv.asInstanceOf[LongPointer]
    case dv: Float8Vector => dv.asInstanceOf[DoublePointer]
    // TODO: missing type
  }

  /*
  private def createVector[T: ClassTag](seq: Seq[T]): Pointer = seq match {
    case s: Seq[Int @unchecked] if classTag[T] == classTag[Int] => new IntVector("Int", new RootAllocator(Long.MaxValue)).asInstanceOf[IntPointer]
    case s: Seq[Double @unchecked] if classTag[T] == classTag[Double]  => new Float8Vector("Double", new RootAllocator(Long.MaxValue)).asInstanceOf[DoublePointer]
    case s: Seq[Long @unchecked] if classTag[T] == classTag[Long] => new BigIntVector("Long", new RootAllocator(Long.MaxValue)).asInstanceOf[LongPointer]
    // TODO: More types
  }
   */

  private def createPointer[T: ClassTag](seq: Array[T]): Pointer = seq match {
    case s: Array[Int @unchecked] if classTag[T] == classTag[Int] => {
      val intVec = new IntPointer(s.length.asInstanceOf[Long])
      intVec.put(s.asInstanceOf[Array[Int]], 0, s.length)
      intVec
    }
    case s: Array[Double @unchecked] if classTag[T] == classTag[Double] => {
      val doublevec = new DoublePointer(s.length.asInstanceOf[Long])
      doublevec.put(s.asInstanceOf[Array[Double]], 0, s.length)
      doublevec
    }
    case s: Array[Long @unchecked] if classTag[T] == classTag[Long] => {
    val longvec = new LongPointer(s.length.asInstanceOf[Long])
    longvec.put(s.asInstanceOf[Array[Long]], 0, s.length)
    longvec
    }
  }

  private def setValue[T: ClassTag](vec: BaseFixedWidthVector, index: Int, value: T) = value match {
    case v: Int => vec.asInstanceOf[IntVector].set(index, v)
    case v: Double => vec.asInstanceOf[Float8Vector].set(index, v)
    case v: Long => vec.asInstanceOf[BigIntVector].set(index, v)
    // TODO: More types
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

class VectorizedRDDAdapter[T: ClassTag](prev: VectorizedRDD[T]) extends VeRDD[T](prev) {
  override def getVectorData = prev.getVectorData
}

object VectorizedRDDAdapter {
  implicit def toVeRDDAdapter[T: ClassTag](r: VectorizedRDD[T]): VectorizedRDDAdapter[T] = new VectorizedRDDAdapter[T](r)
}

/**
 * An RDD that serves as abstract base class for transformations, that run on the vector engine.
 * Default implementations for operations that run on the vector enigne are implemented here.
 *
 * @param prev The previous VectorizedRDD node in the DAG
 * @param classTag$T$0 The information about the erased class of type T
 * @tparam T The type parameter of (for now scalar) data to operate on, i.e. Int, Double, Long, ...
 */
abstract class VeRDD[T: ClassTag](prev: RDD[T] with Vectorized) extends RDD[T](prev) with Vectorized {
  @transient val transpiler: CppTranspiler.type = CppTranspiler


  // subclasses need to implement this
  // this (along with other operations) is where calculations on the VE are triggered

  /**
   * The method to perform the transformation on the vector engine.
   * Decendants implement this to transform that data of the previous node in the DAG
   * into the target format.
   * @return The transformed data
   */
  def getVectorData(): RDD[VeColBatch]

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

    val results = this.getVectorData().mapPartitions { inputIterator =>
      //val start3 = System.nanoTime()
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val newLibRef = veProcess.loadLibrary(Paths.get(newCompiledPath))
      val iter2 = inputIterator.toList.map { batch =>
        evalFunction(newFunc, newLibRef, batch.cols, newOutputs)
      }
      println("c")

      //val end3 = System.nanoTime()
      //println(s"reducing... took ${(end3 - start3) / 1000000000.0}s ")
      iter2.toIterator
    }

    val end1 = System.nanoTime()

    println(s"reduce evalFunction took ${(end1 - start1) / 1000000000.0}s")

    results.mapPartitions { veColBatch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val batches = veColBatch.toList
      val r = batches.flatMap(_.cols).flatMap { veColVector =>
        val intVec = veColVector.toArrowVector().asInstanceOf[IntVector]
        val ids = (0 until intVec.getValueCount)
        ids.map(intVec.get).toList    // Value at index is null
      }
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r.toIterator
    }.collect().sum
  }

  override def reduce(f: (T, T) => T): T = {

    val out = this.getVectorData().mapPartitions { veColBatchIt =>

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
object VectorizedRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VectorizedRDD[T] = new VectorizedRDD[T](r)
}
