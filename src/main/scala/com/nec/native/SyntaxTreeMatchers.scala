package com.nec.native

import scala.reflect.runtime.universe._

final case class IfOnlyStatement(cond: Tree, thenp: Tree)

object IfOnlyStatement {
  def unapply(tree: Tree): Option[IfOnlyStatement] = {
    PartialFunction.condOpt(tree) {
      case If(cond, thenp, Literal(Constant(()))) =>
        IfOnlyStatement(cond, thenp)
    }
  }
}

final case class WhileLoop(label: String, condition: Tree, body: List[Tree])

object WhileLoop {
  def unapply(tree: Tree): Option[WhileLoop] = {
    PartialFunction.condOpt(tree) {
      case LabelDef(
        TermName(label1),
        List(),
        If(cond, Block(children, Apply(Ident(TermName(label2)), List())), Literal(Constant(())))
      ) if (label1.startsWith("while$") && label1 == label2) =>
        WhileLoop(label1, cond, children)
    }
  }
}

final case class DoWhileLoop(label: String, condition: Tree, body: List[Tree])

object DoWhileLoop {
  def unapply(tree: Tree): Option[DoWhileLoop] = {
    PartialFunction.condOpt(tree) {
      case LabelDef(
        TermName(label1),
        List(),
        Block(
          children,
          If(cond, Apply(Ident(TermName(label2)), List()), Literal(Constant(())))
        )
      ) if (label1.startsWith("doWhile$") && label1 == label2) =>
        DoWhileLoop(label1, cond: Tree, children)
    }
  }
}
