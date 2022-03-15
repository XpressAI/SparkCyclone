package com.nec.ve

import com.nec.native.CppTranspiler
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CodeStructure.CodeSection

import scala.reflect.ClassTag
import scala.language.implicitConversions
import org.apache.spark.rdd.RDD

class VectorizedRDD[T](rdd: RDD[T]) {

  import scala.reflect.runtime.universe._

  val transpiler = CppTranspiler

  //def vemap[U:ClassTag](f: (T) => U ): RDD[U] = {

  def vemap[U:ClassTag](expr: Expr[T => T]): RDD[T] = {
    import scala.reflect.runtime.universe._

    // TODO: for inspection, remove when done
    println("vemap got expr: " + showRaw(expr.tree))

    // TODO: Get AST (Expr) from symbol table, when necessary
    // val expr = ...

    // transpile f to C
    val code = transpiler.transpile(expr)
    //println("Generated code:\n" + code)

    // TODO: Embed generated code into mapping function
    val codeLinesFn = CodeLines.parse(code)
    println("Codelines: " + codeLinesFn)

    //
    // val codeLinesMain = CodeLines.from()



    // TODO: remove dummy result
    rdd
  }
}

// implicit conversion
object VectorizedRDD {
  implicit def rddToVectorizedRDD[T](r: RDD[T]) = new VectorizedRDD(r)
}