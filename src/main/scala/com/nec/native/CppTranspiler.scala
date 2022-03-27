package com.nec.native

import com.nec.spark.agile.core.CodeLines

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object CppTranspiler {
  def transpileReduce[T](expr: universe.Expr[(T, T) => T], klass: Class[_]): String = {
    expr.tree match {
      case fun @ Function(vparams, body) => evalReduce(fun, klass)
    }
  }

  def transpileFilter[T](expr: universe.Expr[ T => Boolean], klass: Class[_]): String = {
    expr.tree match {
      case fun @ Function(vparams, body) => evalFilter(fun, klass)
    }
  }

  var functionNames: List[String] = List()

  // entry point
  def transpile[T](expr: Expr[T], klass: Class[_]): String = {
    expr.tree match {
      case fun @ Function(vparams, body) => evalFunc(fun, klass)
    }
  }

  // evaluate Function type from Scala AST
  def evalReduce(fun: Function, klass: Class[_]): String = {
    val resultType: String = resultTypeForKlass(klass)

    val defs = fun.vparams
    fun.body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}_in[0]->count;",
        s"out[0] = $resultType::allocate();",
        s"out[0]->resize(1);",
        s"${evalScalarType(defs.head.tpt, klass)} ${defs.head.name}{};",
        s"${evalScalarType(defs.tail.head.tpt, klass)} ${defs.tail.head.name}{};",
        "for (int i = 0; i < len; i++) {",
        CodeLines.from(
          s"${defs.head.name} = ${defs.head.name}_in[0]->data[i];",
          s"${defs.tail.head.name} = ${evalApplyScalar(apply)};",
        ).indented,
        "}",
        s"out[0]->data[0] = ${defs.tail.head.name};",
        s"out[0]->set_validity(0, 1);",
        //s"""std::cout << "out[0]->data[0] = " << out[0]->data[0] << std::endl;""",
      ).indented.cCode
      case unknown => showRaw(unknown)
    }
  }

  private def resultTypeForKlass(klass: Class[_]) = {
    val resultType = if (klass == classOf[Int]) {
      "nullable_int_vector"
    } else if (klass == classOf[Long]) {
      "nullable_bigint_vector"
    } else if (klass == classOf[Float]) {
      "nullable_float_vector"
    } else {
      "nullable_double_vector"
    }
    resultType
  }

  private def filter_code(defs: List[ValDef], predicate_code: String, klass: Class[_]) = {
    val resultType: String = resultTypeForKlass(klass)
    CodeLines.from(
      s"size_t len = ${defs.head.name.toString}_in[0]->count;",
      s"std::vector<size_t> bitmask(len);",
      s"${evalScalarType(defs.head.tpt, klass)} ${defs.head.name}{};",
      s"for (int i = 0; i < len; i++) {",
      CodeLines.from(
        s"${defs.head.name} = ${defs.head.name}_in[0]->data[i];",
        s"bitmask[i] = ${predicate_code};",
      ).indented,
      s"}",
      s"std::vector<size_t> matching_ids = cyclone::bitmask_to_matching_ids(bitmask);",
      s"out[0] = ${defs.head.name.toString}_in[0]->select(matching_ids);",
    ).indented.cCode
  }

  def evalFilter(fun: Function, klass: Class[_]): String = {
    val resultType: String = resultTypeForKlass(klass)
    val defs = fun.vparams
    fun.body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => filter_code(defs, evalApply(apply), klass)
      case select @ Select(tree, name) => filter_code(defs, evalSelect(select), klass)
      case lit @ Literal(Constant(true)) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}_in[0]->count;",
        s"out[0] = $resultType::allocate();",
        s"out[0]->resize(len);",
        s"${evalScalarType(defs.head.tpt, klass)} ${defs.head.name}{};",
        "for (int i = 0; i < len; i++) {",
        CodeLines.from(
          s"out[0]->data[i] = x_in[0]->data[i];"
        ).indented,
        "}",
        s"out[0]->set_validity(0, len);",
      ).indented.cCode
      case lit @ Literal(Constant(false)) => CodeLines.from(
          s"size_t len = ${defs.head.name.toString}_in[0]->count;",
          s"out[0] = $resultType::allocate();",
          s"out[0]->resize(0);",
          s"out[0]->set_validity(0, 0);",
        ).indented.cCode
      case unknown => showRaw(unknown)
    }
  }

  // evaluate Function type from Scala AST
  def evalFunc(fun: Function, klass: Class[_]): String = {
    evalBody(fun.vparams, fun.body, klass)
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

  def evalScalarType(tree: Tree, klass: Class[_]): String = {

    tree match {
      case ident @ Ident(_) =>
        val idStr = evalIdent(ident)
        idStr match {
          //case "Byte" => "int8_t"       // TODO: Reason about mapping small values to VE
          //case "Short" => "int16_t"
          case "Int" => "int32_t"
          case "Long" => "int64_t"
          case "Float" => "float"
          case "Double" => "double"
          case _ => {
            if (klass == classOf[Int]) {
              "int32_t"
            } else if (klass == classOf[Long]) {
              "int64_t"
            } else if (klass == classOf[Float]) {
              "float"
            } else {
              "double"
            }
          }
        }

      case unknown => {
        if (klass == classOf[Int]) {
          "int32_t"
        } else if (klass == classOf[Long]) {
          "int64_t"
        } else if (klass == classOf[Float]) {
          "float"
        } else {
          "double"
        }
      }
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
  def evalBody(defs: List[ValDef], body: Tree, klass: Class[_]): String = {
    val resultType: String = resultTypeForKlass(klass)

    body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}_in[0]->count;",
        s"out[0] = $resultType::allocate();",
        s"out[0]->resize(len);",
        defs.map { d =>
          s"${evalScalarType(d.tpt, klass)} ${d.name}{};"
        },
        "for (int i = 0; i < len; i++) {",
        CodeLines.from(
          defs.map { d =>
            s"${d.name} = ${d.name}_in[0]->data[i];"
          },
          s"out[0]->data[i] = ${evalApply(apply)};",
          //s"""std::cout << a_in[0]->data[i] << std::endl;""",
          s"out[0]->set_validity(i, 1);"
        ).indented,
        "}",
      ).indented.cCode
      case unknown => showRaw(unknown)
    }

  }

  def evalIdent(ident: Ident): String = {
    ident match {
      case other => s"${other}"
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

  def evalApplyScalar(apply: Apply): String = {

    val funStr = apply.fun match {
      case sel @ Select(tree, name) => evalSelectScalar(sel)
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
      case Constant(true) => "1"
      case Constant(false) => "0"
      case Constant(c) => c.toString
      case unknown => "<unknown args in Literal: " + showRaw(unknown) + ">"
    }
  }

  def evalSelect(select: Select): String = {

    select.name match {
      case TermName("unary_$bang") => " !" + evalQual(select.qualifier)
      case _ => evalQual(select.qualifier) + evalName(select.name)
    }

  }

  def evalSelectScalar(select: Select): String = {

    evalQualScalar(select.qualifier) + evalName(select.name)

  }

  def evalQual(tree: Tree): String = {

    tree match {
      case ident @ Ident(_) => evalIdent(ident)
      case apply @ Apply(x) => evalApply(apply)
      case lit @ Literal(_) => evalLiteral(lit)
      case unknown => "<unknown qual: " + showRaw(unknown) + ">"
    }

  }
  def evalQualScalar(tree: Tree): String = {

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
      case TermName("$less") => " < "
      case TermName("$less$eq") => " <= "
      case TermName("$greater") => " > "
      case TermName("$greater$eq") => " >= "
      case TermName("$eq$eq") => " == "
      case TermName("$bang$eq") => " != "
      case TermName("$percent") => " % "
      case TermName("unary_$bang") => " !"
      case TermName("$amp$amp") => " && "
      case TermName("$bar$bar") => " || "
      case unknown => "<< <UNKNOWN> in evalName>>"
    }
  }

}

