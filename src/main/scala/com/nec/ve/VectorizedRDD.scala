package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.SparkCycloneDriverPlugin
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

class VectorizedRDD[T](rdd: RDD[T]) {

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
    var code = transpiler.transpile(expr)
    println("Generated code:\n" + code)

    // TODO: Embed generated code into mapping function more elegantly
    code = "#include <cstdint>\n\nusing namespace std;\n\n" + code

    println("code: " + code)

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(code)
    println("compiled path:" + compiledPath)

    // TODO: remove dummy result
    rdd
  }
}

// implicit conversion
object VectorizedRDD {
  implicit def toVectorizedRDD[T](r: RDD[T]): VectorizedRDD[T] = new VectorizedRDD(r)
}