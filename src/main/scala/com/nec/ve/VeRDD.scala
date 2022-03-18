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
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}
import org.bytedeco.javacpp.{DoublePointer, IntPointer}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

class VeRDD[T: ClassTag](rdd: RDD[T]) extends RDD[T](rdd) {
  @transient val transpiler: CppTranspiler.type = CppTranspiler

  private def convert[U: ClassTag](vals: Array[U]): List[VeColVector] = {
    import com.nec.spark.SparkCycloneExecutorPlugin._ // needed for veProcess (implicit)
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext // needed for OriginalCallingContext

    vals match {
      case values: Array[Int @unchecked] if classTag[U] == classTag[Int] => {
        val intVec = new IntPointer(values.length.asInstanceOf[Long])
        intVec.put(values, 0, values.length)
        List(VeColVector.fromIntPointer(intVec))
      }
      case values: Array[Double @unchecked] if classTag[U] == classTag[Double] => {
        val doubleVec = new DoublePointer(values.length.asInstanceOf[Long])
        doubleVec.put(values, 0, values.length)
        List(VeColVector.fromDoublePointer(doubleVec))
      }
    }
  }

  val inputs: RDD[VeColBatch] = rdd.mapPartitions { valsIter =>
    println("Reading inputs")
    val start = System.nanoTime()
    import com.nec.spark.SparkCycloneExecutorPlugin._
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    val vals = valsIter.toArray
    val len = vals.length

    //val intVec = new IntPointer(len.asInstanceOf[Long])
    //intVec.put(vals, 0, vals.length)

    val converted = convert(vals)

    /*val intVec = new IntVector("foo", new RootAllocator(Int.MaxValue))
    intVec.allocateNew()
    intVec.setValueCount(vals.length)
    vals.zipWithIndex.foreach { case (v, i) =>
      intVec.set(i, v.asInstanceOf[Int])
    }
    */
    val end = System.nanoTime()
    println(s"Took ${(end - start) / 1000000000}s to convert ${vals.length} rows.")

    Iterator(VeColBatch.fromList(converted))
    //Iterator(VeColBatch.fromList(List(VeColVector.fromPointer(intVec))))
    //Iterator(VeColBatch.fromList(List(VeColVector.fromArrowVector(intVec))))
  }.persist(StorageLevel.MEMORY_ONLY)
  val inputCount: Long = inputs.count()

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