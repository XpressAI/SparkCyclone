package com.nec.native

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

    val fnName: String = "function" + functionNames.length.toString
    functionNames = fnName :: functionNames

    var returnType =  "void"
    if (fun.tpe != null) {
      // TODO: map type to propper C type
      returnType = fun.tpe.toString
    }

    val paramStr = evalVParams(fun.vparams)
    val bodyStr = evalBody(fun.body)

    returnType + " " + fnName + "(" + paramStr + ") { " + bodyStr + " }"
  }

  // evaluate vparams for Function
  def evalVParams(defs: List[ValDef]): String = {

    defs.map( (v: ValDef) => {

      val modStr = evalModifiers(v.mods)

      val nameStr = v.name.toString

      val typeStr = evalType(v.tpt)

      val rhsStr = v.rhs.toString()

      nameStr + ": " + typeStr
    }).mkString(", ")

  }

  def evalType(tree: Tree): String = {

    tree match {
      case ident @ Ident(_) => {
        val idStr = evalIdent(ident)
        idStr match {
          case "Byte" => "int8_t"       // TODO: Reason about mapping small values to VE
          case "Short" => "int16_t"
          case "Int" => "int32_t"
          case "Long" => "int64_t"
          case "Float" => "float"
          case "Double" => "double"
          case unknown => "<unhandled type: " + idStr + ">"
        }
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
  def evalBody(tree: Tree): String = {

    tree match {
      case ident @ Ident(name) => "return " + evalIdent(ident) + ";"
      case apply @ Apply(fun, args) => "return " + evalApply(apply) + ";"
      case unknown => showRaw(unknown)
    }

  }

  def evalIdent(ident: Ident): String = {


    ident match {
      case other => other.toString
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

