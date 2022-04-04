package com.nec.native

import com.nec.native.SyntaxTreeOps._

import scala.reflect.runtime.universe._

object FunctionReformatter {
  def flattenParams(params: List[ValDef],
                    nameGenerator: String => String = paramNameGenerator): List[ValDef] = {
    val flattenedArgTypes = params
      .map(_.tpt.asInstanceOf[TypeTree].tpe)
      .flatMap { tpe =>
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
      val name = TermName(nameGenerator.apply(s"_${i + 1}"))
      q"${name}: ${tpe}"
    }

    // Create a dummy function with the argument terms to construct the new params list
    q"(..$terms) => 0".vparams
  }

  def reformatMapBody(func: Function,
                      nameGenerator: String => String = paramNameGenerator): Tree = {
    val transformer = new Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          // NOTE: May need to update this to handle multi-parameter functions
          case Select((Ident(i)), TermName(term)) if i == func.vparams.head.name && term.matches("_\\d?\\d$") =>
            Ident(TermName(nameGenerator.apply(term)))

          case i: Ident if (i.name == func.vparams.head.name) =>
            Ident(TermName(nameGenerator.apply("_1")))

          case _ =>
            super.transform(tree)
        }
      }
    }

    transformer.transform(func.body)
  }

  def reformatReduceBody(func: Function,
                         mapNameGenerator: String => String,
                         aggNameGenerator: String => String
                        ): Tree = {
    val tupleSuffix = "_\\d?\\d$"
    val transformer = new Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          // Handle input names
          case Select((Ident(i)), TermName(term)) if i == func.vparams.head.name && term.matches(tupleSuffix) =>
            Ident(TermName(mapNameGenerator.apply(term)))
          case i: Ident if (i.name == func.vparams.head.name) =>
            Ident(TermName(mapNameGenerator.apply("_1")))

          // Handle agg Names
          case Select((Ident(i)), TermName(term)) if i == func.vparams(1).name && term.matches(tupleSuffix) =>
            Ident(TermName(aggNameGenerator.apply(term)))
          case i: Ident if (i.name == func.vparams(1).name) =>
            Ident(TermName(aggNameGenerator.apply("_1")))

          case _ =>
            super.transform(tree)
        }
      }
    }

    transformer.transform(func.body)
  }

  def reformatFunction(expr: Expr[_]): Function = {
    val toolbox = CompilerToolBox.get

    toolbox.typecheck(expr.tree) match {
      // Map-like expressions: One input (which may be simple or a tuple) and
      // one output (which still may be simple or a tuple)
      case func @ Function(vparams, _) if vparams.size == 1 =>
        // Return the new tree in typechecked form
        toolbox.typecheck(
          Function(
            flattenParams(vparams),
            /*
              After the transformations, the type information will be lost even
              though the body's tree is considered typchecked.
            */
            toolbox.untypecheck(reformatMapBody(func))
          )
        ).asInstanceOf[Function]

      // Reduce-like expressions: First input is the actual method input,
      // second input is for the aggregation
      case func @ Function(vparams, _) if vparams.size == 2 =>
        val aggNameGenerator: String => String = (i) => s"agg${i}"
        toolbox.typecheck(
          Function(
            flattenParams(vparams.take(1)) ++ flattenParams(vparams.drop(1), aggNameGenerator),
            /*
              After the transformations, the type information will be lost even
              though the body's tree is considered typchecked.
            */
            toolbox.untypecheck(reformatReduceBody(func, paramNameGenerator, aggNameGenerator))
          )
        ).asInstanceOf[Function]
      case func @ Function(vparams, _) =>
        throw new IllegalArgumentException(s"Unsupported function arity (${vparams.size})")
      case _ =>
        throw new NotImplementedError(s"Function reformatting only works with Function")
    }
  }
}
