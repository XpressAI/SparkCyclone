package com.nec.native

import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.{PointerPointer, Raw}
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector, CodeLines, VeType}
import com.nec.util.DateTimeOps._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}

import java.time.Instant
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe, currentMirror => cm}
import scala.tools.reflect.ToolBox

object CppTranspiler {
  private val toolbox = cm.mkToolBox()

  def transpileGroupBy[T, K](expr: universe.Expr[T => K]): CompiledVeFunction = {
    val resultType = expr.staticType.typeArgs.last

    val code = expr.tree match {
      case fun @ Function(vparams, body) => evalGroupBy(fun, resultType)
    }

    val funcName = s"groupby_${Math.abs(code.hashCode())}"
    val dataType = sparkTypeToVeType(resultType)
    val newOutputs = List(CVector("out", dataType))

    CompiledVeFunction(new CFunction2(
      funcName,
      Seq(
        Raw("size_t input_batch_count"),
        Raw(s"size_t **group_key_pointer"),
        Raw(s"size_t *group_count_pointer"),
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    ), newOutputs)
  }

  def transpileReduce[T](expr: universe.Expr[(T, T) => T]): CompiledVeFunction = {
    val resultType = expr.staticType.typeArgs.last
    val code = expr.tree match {
      case fun @ Function(vparams, body) => evalReduce(fun, resultType)
    }

    val funcName = s"reduce_${Math.abs(code.hashCode())}"
    val dataType = sparkTypeToVeType(resultType)
    val newOutputs = List(CVector("out", dataType))

    CompiledVeFunction(new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    ), newOutputs)
  }

  def transpileFilter[T](expr: universe.Expr[ T => Boolean]): CompiledVeFunction = {
    val resultType = expr.staticType.typeArgs.head

    val code = expr.tree match {
      case fun @ Function(vparams, body) => evalFilter(fun, resultType)
    }

    val funcName = s"filter_${Math.abs(code.hashCode())}"
    val dataType = sparkTypeToVeType(resultType)
    val newOutputs = List(CVector("out", dataType))

    CompiledVeFunction(new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    ), newOutputs)
  }

  def transpileMap[T](expr: Expr[T]): CompiledVeFunction = {
    val resultType = expr.staticType.typeArgs.last

    val code = expr.tree match {
      case fun @ Function(vparams, body) => evalFunc(fun, resultType)
    }

    val funcName = s"eval_${Math.abs(code.hashCode())}"
    val dataType = sparkTypeToVeType(resultType)
    val newOutputs = List(CVector("out", dataType))

    CompiledVeFunction(new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", dataType)),
        PointerPointer(newOutputs.head)
      ),
      code,
      DefaultHeaders
    ), newOutputs)
  }

  def sparkTypeToVeType(t: Type): VeType = Map(
    typeOf[Int] -> SparkExpressionToCExpression.sparkTypeToVeType(IntegerType),
    typeOf[Long] -> SparkExpressionToCExpression.sparkTypeToVeType(LongType),
    typeOf[Float] -> SparkExpressionToCExpression.sparkTypeToVeType(FloatType),
    typeOf[Double] -> SparkExpressionToCExpression.sparkTypeToVeType(DoubleType),
    typeOf[Instant] -> SparkExpressionToCExpression.sparkTypeToVeType(LongType),
  )(t)

  def evalGroupBy(fun: Function, t: Type): String = {
    val resultType: String = resultTypeForType(t)
    val defs = fun.vparams

    fun.body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => groupByCode(defs, evalApply(apply), t)
      case select @ Select(tree, name) => groupByCode(defs, evalSelect(select), t)
      case unknown => showRaw(unknown)
    }
  }

  // evaluate Function type from Scala AST
  def evalReduce(fun: Function, t: Type): String = {
    val resultType: String = resultTypeForType(t)

    val defs = fun.vparams
    fun.body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}_in[0]->count;",
        s"out[0] = $resultType::allocate();",
        s"out[0]->resize(1);",
        s"${evalScalarType(defs.head.tpt, t)} ${defs.head.name} {};",
        s"${evalScalarType(defs.tail.head.tpt, t)} ${defs.tail.head.name} {};",
        "for (auto i = 0; i < len; i++) {",
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

  private def resultTypeForType(t: Type): String = {
    if (t =:= typeOf[Int]) {
      "nullable_int_vector"
    } else if (t =:= typeOf[Long]) {
      "nullable_bigint_vector"
    } else if (t =:= typeOf[Float]) {
      "nullable_float_vector"
    } else if (t =:= typeOf[Instant]) {
      "nullable_bigint_vector"
    } else {
      "nullable_double_vector"
    }
  }

  private def groupByCode(defs: List[ValDef], predicate_code: String, t: Type) = {
    val resultType: String = resultTypeForType(t)

    CodeLines.from(
      // Merge inputs and assign output to pointer
      s"${evalType(defs.head.tpt)} *tmp = ${evalType(defs.head.tpt)}::merge(${defs.head.name.toString}_in, input_batch_count);",
      s"size_t len = tmp->count;",
      s"std::vector<size_t> grouping(len);",
      s"std::vector<size_t> grouping_keys;",
      s"${evalScalarType(defs.head.tpt, t)} ${defs.head.name} {};",
      s"for (auto i = 0; i < len; i++) {",
      CodeLines.from(
        s"${defs.head.name} = tmp->data[i];",
        s"grouping[i] = ${predicate_code};"
        //s"""std::cout << "bitmask[" << i << "] = " << bitmask[i] << std::endl;"""
      ).indented,
      s"}",
      s"std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, grouping_keys);",
      s"*group_key_pointer = grouping_keys.data();",
      "*group_count_pointer = grouping_keys.size();",
      s"*out = (${resultType} *)malloc(groups.size() * sizeof($resultType *));",
      s"for (auto i = 0; i < groups.size(); i++) {",
      CodeLines.from(
        s"out[i] = tmp->select(groups[i]);"
      ).indented,
      s"}",
      s"free(tmp);"
    ).indented.cCode
  }

  private def filterCode(defs: List[ValDef], predicate_code: String, t: Type) = {
    val resultType: String = resultTypeForType(t)
    CodeLines.from(
      s"size_t len = ${defs.head.name.toString}_in[0]->count;",
      s"std::vector<size_t> bitmask(len);",
      s"${evalScalarType(defs.head.tpt, t)} ${defs.head.name} {};",
      s"for (auto i = 0; i < len; i++) {",
      CodeLines.from(
        s"${defs.head.name} = ${defs.head.name}_in[0]->data[i];",
        s"bitmask[i] = ${predicate_code};"
        //s"""std::cout << "bitmask[" << i << "] = " << bitmask[i] << std::endl;"""
      ).indented,
      s"}",
      s"std::vector<size_t> matching_ids = cyclone::bitmask_to_matching_ids(bitmask);",
      s"out[0] = ${defs.head.name.toString}_in[0]->select(matching_ids);",
    ).indented.cCode
  }

  def evalFilter(fun: Function, t: Type): String = {
    val resultType: String = resultTypeForType(t)
    val defs = fun.vparams
    fun.body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => filterCode(defs, evalApply(apply), t)
      case select @ Select(tree, name) => filterCode(defs, evalSelect(select), t)
      case lit @ Literal(Constant(true)) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}_in[0]->count;",
        s"out[0] = $resultType::allocate();",
        s"out[0]->resize(len);",
        s"${evalScalarType(defs.head.tpt, t)} ${defs.head.name} {};",
        "for (auto i = 0; i < len; i++) {",
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
  def evalFunc(fun: Function, t: Type): String = {
    evalBody(fun.vparams, fun.body, t)
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
          case "Instant" => "nullable_bigint_vector"
          case unknown => "<unhandled type: " + idStr + ">"
        }

      case unknown => "<unknown type: " + showRaw(unknown) + ">"
    }

  }

  def evalScalarType(tree: Tree, t: Type): String = {

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
          case "Instant" => "int64_t"
          case _ => {
            if (t =:= typeOf[Int]) {
              "int32_t"
            } else if (t =:= typeOf[Long]) {
              "int64_t"
            } else if (t =:= typeOf[Float]) {
              "float"
            } else if (t =:= typeOf[Instant]) {
              "int64_t"
            } else {
              "double"
            }
          }
        }

      case unknown => {
        if (t =:= typeOf[Int]) {
          "int32_t"
        } else if (t =:= typeOf[Long]) {
          "int64_t"
        } else if (t =:= typeOf[Float]) {
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
  def evalBody(defs: List[ValDef], body: Tree, t: Type): String = {
    val resultType: String = resultTypeForType(t)

    body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => CodeLines.from(
        s"size_t len = ${defs.head.name.toString}_in[0]->count;",
        s"out[0] = $resultType::allocate();",
        s"out[0]->resize(len);",
        defs.map { d =>
          s"${evalScalarType(d.tpt, t)} ${d.name} {};"
        },
        "for (auto i = 0; i < len; i++) {",
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

  def evalArg(arg: Tree): String = {
    arg match {
      case literal @ Literal(_) => evalLiteral(literal)
      case ident @ Ident(_) => evalIdent(ident)
      case apply @ Apply(_) => evalApply(apply)
      case unknown => "<unknown args in apply: " + showRaw(unknown) + ">"
    }
  }

  def evalApply(apply: Apply): String = {
    PartialFunction.condOpt(apply.fun) {
      /*
        Handle the case of calling static methods of `java.time.Instant` to
        instantiate a timestamp by evaluating them in real-time, e.g. `Instant.now`
      */
      case Select(y, _) if Option(y.symbol).map(_.fullName) == Some(classOf[java.time.Instant].getName) =>
        toolbox.eval(apply)
          .asInstanceOf[Instant]
          .toFrovedisDateTime
          .toString
    }
    .orElse {
      /*
        Handle the case of calling `Comparable.comareTo`, which will translate
        into a sequenced ternary operator instead of binary arithmetic operator
        in C++
      */
      (apply.fun, apply.args) match {
        case (Select(tree, TermName("compareTo")), arg :: Nil) =>
          val left = evalArg(tree)
          val right = evalArg(arg)
          Some(s"((${left} == ${right}) ? 0 : (${left} < ${right}) ? -1 : 1)")

        case _ =>
          None
      }
    }
    .getOrElse {
      /*
        Handle the default case
      */
      val funStr = apply.fun match {
        case sel @ Select(tree, name) => evalSelect(sel)
        case unknown => "unknown fun in evalApply: " + showRaw(unknown)
      }
      val argsStr = apply.args.map(evalArg).mkString(", ")

      "(" + funStr + argsStr + ")"
    }
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

