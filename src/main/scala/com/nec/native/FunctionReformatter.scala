package com.nec.native

import com.nec.native.SyntaxTreeOps._
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object FunctionReformatter {
  def flattenParams(func: Function): List[ValDef] = {
    val flattenedArgTypes = func.argTypes.flatMap { tpe =>
      if (tpe.typeConstructor <:< typeOf[Product]) {
        // If the argument is a tuple, expand the types out
        tpe.typeArgs
      } else {
        // Else return the argument's type
        Seq(tpe)
      }
    }

    // Generate the quasistring chunk for `(x: Type)`
    val terms = flattenedArgTypes.zipWithIndex.map { case (tpe, i) =>
      val name = TermName(s"in_${i + 1}_val")
      q"${name}: ${tpe}"
    }

    // Create a dummy function with the argument terms to construct the new params list
    q"(..$terms) => 0".vparams
  }

  def reformatBody(func: Function): Tree = {
    val transformer = new Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          // NOTE: May need to update this to handle multi-parameter functions
          case Select((Ident(i)), TermName(term)) if i == func.vparams.head.name && term.matches("_\\d?\\d$") =>
            Ident(TermName(s"in${term}_val"))

          case i: Ident if (i.name == func.vparams.head.name) =>
            Ident(TermName("in_1_val"))

          case _ =>
            super.transform(tree)
        }
      }
    }

    transformer.transform(func.body)
  }

  def reformatFunction[T, U](expr: Expr[T => U]): Function = {
    val toolbox = expr.mirror.mkToolBox()

    toolbox.typecheck(expr.tree) match {
      // NOTE: For now, this is always the case bc the method is type-constrained with T and U
      case func @ Function(vparams, _) if vparams.size > 0 =>
        // Return the new tree in typechecked form
        toolbox.typecheck(
          Function(
            flattenParams(func),
            /*
              After the transformations, the type information will be lost even
              though the body's tree is considered typchecked.
            */
            toolbox.untypecheck(reformatBody(func))
          )
        ).asInstanceOf[Function]

      case func @ Function(vparams, _) =>
        toolbox.typecheck(
          Function(
            vparams,
            toolbox.untypecheck(reformatBody(func))
          )
        ).asInstanceOf[Function]

      case _ =>
        throw new NotImplementedError(s"Function reformatting only works with Function")
    }
  }
}
