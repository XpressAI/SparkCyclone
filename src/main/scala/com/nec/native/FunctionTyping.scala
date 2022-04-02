package com.nec.native

import com.nec.native.SyntaxTreeOps._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

case class TypeContainer[T](tpe: Type, tag: ClassTag[T], ordering: Ordering[T])
case class FunctionTyping[I, O](input: TypeContainer[I], output: TypeContainer[O])

object FunctionTyping {
  def fromExpression[I, O](expr: Expr[_]): FunctionTyping[I, O] = {
    val toolbox = expr.mirror.mkToolBox()
    toolbox.typecheck(expr.tree) match {
      case f: Function => {
        val input = extractTypes[I](f.vparams.head.tpt.tpe, toolbox)
        val output = extractTypes[O](f.returnType, toolbox)
        FunctionTyping(input, output)
      }
      case _ => throw new IllegalArgumentException(s"Given Expression ($expr) is not a function!")
    }
  }

  private def extractTypes[T](tpe: Type, toolBox: ToolBox[universe.type]): TypeContainer[T] = {
    val ctag = ClassTag[T](toolBox.mirror.runtimeClass(tpe))
    val ordering = toolBox.eval(q"Ordering[$tpe]").asInstanceOf[Ordering[T]]
    TypeContainer(tpe, ctag, ordering)
  }
}
