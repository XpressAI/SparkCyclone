package com.nec.native

import com.nec.spark.agile.core.CodeLines

import scala.reflect.runtime.universe._

object CppTranspiler {

  var functionNames: List[String] = List()

  // entry point
  def transpile[T](expr: Expr[T]): String = {
    expr.tree match {
      case fun @ Function(vparams, body) => evalFunc(fun)
    }
  }

  // evaluate Function type from Scala AST
  def evalFunc(fun: Function): String = {
    evalBody(fun.vparams, fun.body)
  }

  // evaluate vparams for Function
  def evalVParams(fun: Function): String = {
    val defs = fun.vparams
    val inputs = defs.map { (v: ValDef) =>
      val modStr = evalModifiers(v.mods)
      val nameStr = v.name.toString
      val typeStr = evalType(v.tpt)
      val rhsStr = v.rhs.toString
      typeStr + " " + nameStr
    }
    val output = if (fun.tpe == null) {
      List(s"${evalType(defs.head.tpt)} out")
    } else {
      List(s"${fun.tpe} out")
    }
    (inputs ++ output).mkString(", ")
  }

  def evalType(tree: Tree): String = {
    tree match {
      case ident @ Ident(_) =>
        val idStr = evalIdent(ident)
        idStr match {
          //case "Byte" => "int8_t"       // TODO: Reason about mapping small values to VE
          //case "Short" => "int16_t"
          case "Int" => "nullable_int_vector"
          case "Long" => "nullable_bigint_vector"
          case "Float" => "nullable_float_vector"
          case "Double" => "nullable_double_vector"
          case unknown => "<unhandled type: " + idStr + ">"
        }

      case unknown => "<unknown type: " + showRaw(unknown) + ">"
    }

  }

  // currently not used
  def evalModifiers(modifiers: Modifiers): String = {

    // TODO: Annotations

    // TODO: Flags (should contain PARAM for parameters)
    modifiers.flags

    // TODO: Visibility scope

    ""
  }

  // evaluate the body of a function
  def evalBody(defs: List[ValDef], body: Tree): String = {

    body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}[0]->count;",
        s"out[0] = nullable_int_vector::allocate();",
        s"out[0]->resize(len);",
        "for (int i = 0; i < len; i++) {",
        CodeLines.from(
          s"out[0]->data[i] = ${evalApply(apply)};",
          s"out[0]->set_validity(i, 1);"
        ).indented,
        "}",
      ).indented.cCode
      case unknown => showRaw(unknown)
    }

  }

  def evalIdent(ident: Ident): String = {

    ident match {
      case other => s"${other}[0]->data[i]"
    }
  }

  def evalApply(apply: Apply): String = {

    val funStr = apply.fun match {
      case sel @ Select(tree, name) => evalSelect(sel)
      case unknown => "unknown fun in evalApply: " + showRaw(unknown)
    }

    val argsStr = apply.args.map({
      case literal @ Literal(_) => evalLiteral(literal)
      case ident @ Ident(_) => evalIdent(ident)
      case apply @ Apply(_) => evalApply(apply)
      case unknown => "<unknown args in apply: " + showRaw(unknown) + ">"
    }).mkString(", ")

     "(" + funStr + argsStr + ")"
  }

  def evalLiteral(literal: Literal): String = {
    literal.value match {
      case Constant(c) => c.toString
      case unknown => "<unknown args in Literal: " + showRaw(unknown) + ">"
    }
  }

  def evalSelect(select: Select): String = {

    evalQual(select.qualifier) + evalName(select.name)

  }

  def evalQual(tree: Tree): String = {

    tree match {
      case ident @ Ident(_) => evalIdent(ident)
      case apply @ Apply(x) => evalApply(apply)
      case lit @ Literal(_) => evalLiteral(lit)
      case unknown => "<unknown qual: " + showRaw(unknown) + ">"
    }

  }

  def evalName(name: Name): String = {
    name match {
      case TermName("$plus") => " + "
      case TermName("$minus") => " - "
      case TermName("$times") => " * "
      case TermName("$div") => " / "
      case unknown => "<< <UNKNOWN> in evalName>>"
    }
  }

}

