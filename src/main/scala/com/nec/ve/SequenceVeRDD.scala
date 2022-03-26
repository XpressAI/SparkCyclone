package com.nec.ve

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector, VeNullableLong}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.VeRDD.vemap_impl
import com.nec.ve.colvector.VeColVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


class SequenceVeRDD[T: ClassTag](
  rdd: RDD[T],
  start: Long,
  end: Long,
  step: Long = 1
)(implicit tag: WeakTypeTag[T]) extends VeRDD[T](rdd) {
  override def map(f: T => T): MappedVeRDD[T] = macro vemap_impl[T]

  def vemap(expr: Expr[T => T]): MappedVeRDD[T] = {

    // transpile f to C
    val code = s"""
      |  out[0] = nullable_long_vector::allocate();
      |  out[0]->resize($end);
      |  for (size_t i = $start; i < $end; i++) {
      |    out[0]->data[i] = i * $step;
      |  }
      |  for (size_t i = $start; i < $end; i++) {
      |    out[0]->set_validity(i, 1);
      |  }
      |""".stripMargin

    val funcName = s"seq_${Math.abs(code.hashCode())}"

    val outputs = List(CVector("out", VeNullableLong))
    val func = new CFunction2(
      funcName,
      Seq(
        PointerPointer(outputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    sc.parallelize()
    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    new VeRDD(this, func, compiledPath.toAbsolutePath.toString, outputs)
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


