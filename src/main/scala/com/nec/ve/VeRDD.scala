package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.Pointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.ve.VeProcess.OriginalCallingContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.language.implicitConversions
import scala.reflect.ClassTag

class VeRDD[T: ClassTag](rdd: RDD[T]) extends RDD[T](rdd) {
  var func: CFunction2 = _

  import scala.reflect.runtime.universe._

  val transpiler: CppTranspiler.type = CppTranspiler

  //def vemap[U:ClassTag](f: (T) => U ): RDD[U] = {

  def vemap[U:ClassTag](expr: Expr[T => T]): RDD[T] = {
    import scala.reflect.runtime.universe._

    // TODO: for inspection, remove when done
    println("vemap got expr: " + showRaw(expr.tree))

    // TODO: Get AST (Expr) from symbol table, when necessary
    // val expr = ...

    // transpile f to C
    val code = transpiler.transpile(expr)
    val funcName = s"eval_${Math.abs(code.hashCode())}"

    func = new CFunction2(
      funcName,
      Seq(
        Pointer(CVector("a", CFunctionGeneration.VeScalarType.veNullableInt)),
        Pointer(CVector("out", CFunctionGeneration.VeScalarType.veNullableInt))
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders)
    println("compiled path:" + compiledPath)

    // TODO: remove dummy result
    new MappedVeRDD(this)
  }

  override def collect(): Array[T] = super.collect()

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    ??? // TODO
  }

  override protected def getPartitions: Array[Partition] = {
    ??? // TODO
  }

  def withCompiled[U](cCode: String)(f: Path => U): U = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
        .compile_c(cCode)
    f(oPath)
  }

  var inputs: VeColBatch = _
  var outVectors: List[CVector] = _
  var lastResult: List[VeColBatch.VeColVector] = _

  def evalFunction()(implicit ctx: OriginalCallingContext): Unit = {
    withCompiled(func.toCodeLinesWithHeaders.cCode) { path =>
      import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

      val libRef = veProcess.loadLibrary(path)
      lastResult = veProcess.execute(libRef, func.name, inputs.cols, outVectors)
    }
  }
}

// implicit conversion
object VeRDD {
  implicit def toVectorizedRDD[T: ClassTag](r: RDD[T]): VeRDD[T] = new VeRDD(r)
}