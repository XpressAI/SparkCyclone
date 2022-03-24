package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core._
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.{SparkCycloneDriverPlugin, SparkCycloneExecutorPlugin}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.colvector.VeColVector
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, IntVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class VeRDD[T: ClassTag](rdd: RDD[T]) extends RDD[T](rdd) {
  @transient val transpiler: CppTranspiler.type = CppTranspiler

  val jobRdd: RDD[Int] = sparkContext.parallelize(0 until 8).repartition(8)
  val inputs: RDD[VeColBatch] = rdd.mapPartitionsWithIndex { case (index, valsIter) =>
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    if (SparkCycloneExecutorPlugin.containsCachedBatch("inputs")) {
      println(s"Using cached inputs for ${index}")

      val valsArray = valsIter.toArray.asInstanceOf[Array[Long]]

      println(s"First value: ${valsArray(0)}")
      println(s"Last value: ${valsArray(valsArray.length - 1)}")

      Iterator(SparkCycloneExecutorPlugin.getCachedBatch(s"inputs"))
    } else {
      println(s"Reading inputs for ${index}")
      val start = System.nanoTime()

      val valsArray = valsIter.toSeq
      println(s"First value: ${valsArray.head}")
      println(s"Last value: ${valsArray.last}")

      val root = new RootAllocator(Int.MaxValue)
      val arrowVector = typeOf[T] match {
        case _: Int =>
          new IntVector("", root)
        case _: Long =>
          new BigIntVector("", root)
        case _: Option[Int] =>
          new IntVector("", root)
        case _: Option[Long] =>
          new BigIntVector("", root)
      }
      arrowVector.setValueCount(valsArray.length)

      for ((v, i) <- valsArray.zipWithIndex) {
        (arrowVector, valsArray) match {
          case (intVector: IntVector, a: Seq[Int]) =>
            intVector.set(i, a(i).asInstanceOf[Int])
          case (intVector: IntVector, v: Seq[Option[Int]]) =>
            v.asInstanceOf[Option[Int]].foreach(x => intVector.set(i, x))
          case (intVector: BigIntVector, a: Seq[Long]) =>
            intVector.set(i, a(i).asInstanceOf[Int])
          case (intVector: BigIntVector, v: Seq[Option[Long]]) =>
            v.asInstanceOf[Option[Int]].foreach(x => intVector.set(i, x))
        }
        val end = System.nanoTime()

        println(
          s"Took ${(end - start) / 1000000000}s to convert ${arrowVector.getValueCount} rows."
        )

        //val batch = VeColBatch.fromList(List(VeColVector.fromPointer(intVec)))
        val batch = VeColBatch.fromList(List(VeColVector.fromArrowVector(arrowVector)))
        SparkCycloneExecutorPlugin.registerCachedBatch("input", batch)
        Iterator(batch)
      }
    }
    // Trigger caching of VeColBatches
    println("Trying to trigger VeColBatch caching.")
    inputs.filter(_ => false).collect()
    println("Finished collect()")
  }
  def vemap[U: ClassTag](expr: Expr[T => T]): MappedVeRDD = {
    import scala.reflect.runtime.universe._

    // TODO: for inspection, remove when done
    println("vemap got expr: " + showRaw(expr.tree))

    // TODO: Get AST (Expr) from symbol table, when necessary
    // val expr = ...

    // transpile f to C
    val code = transpiler.transpile(expr)
    val funcName = s"eval_${Math.abs(code.hashCode())}"

    val outputs = List(CVector("out", VeNullableLong))
    val func = new CFunction2(
      funcName,
      Seq(PointerPointer(CVector("a_in", VeNullableLong)), PointerPointer(outputs.head)),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    // TODO: remove dummy result
    new MappedVeRDD(this, func, compiledPath.toAbsolutePath.toString, outputs)
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
    val oPath =
      VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
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

// implicit conversion
object VeRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VeRDD[T] = new VeRDD(r)
}
