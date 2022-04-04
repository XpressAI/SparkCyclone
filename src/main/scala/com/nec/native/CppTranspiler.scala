package com.nec.native

import com.nec.native.SyntaxTreeOps._
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.{PointerPointer, PointerPointerPointer, Raw}
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector, CodeLines, VeType}
import com.nec.util.DateTimeOps._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}

import java.time.Instant
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object CppTranspiler {
  private val toolbox = CompilerToolBox.get

  def transpileGroupBy[T, K](expr: universe.Expr[T => K]): CompiledVeFunction = {
    val func = FunctionReformatter.reformatFunction(expr)
    val signature = func.veSignature
    require(signature.outputs.size == 1, s"Can not group by anything other than simple types! Got: ${signature.outputs}")

    val resultType = signature.outputs.head

    val code = func match {
      case fun @ Function(vparams, body) => evalGroupBy(fun, signature)
    }

    val funcName = s"groupby_${Math.abs(code.hashCode())}"

    CompiledVeFunction(new CFunction2(
      funcName,
      Seq(
        Raw("size_t input_batch_count"),
        PointerPointer(CVector("groups_out", resultType.veType)),
      ) ++ signature.inputs.map(PointerPointer) ++ signature.inputs.zipWithIndex.map{case (i, idx) => PointerPointerPointer(i.withNewName(s"out_$idx"))},
      code,
      DefaultHeaders
    ),
      // Outputs are for the groups
      signature.inputs,
      // Typing is for the Keying function
      FunctionTyping.fromExpression(expr))
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
    ), newOutputs, FunctionTyping.fromExpression(expr))
  }

  def transpileFilter[T](expr: universe.Expr[T => Boolean]): CompiledVeFunction = {
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
    ), newOutputs, FunctionTyping.fromExpression(expr))
  }

  case class VeSignature(inputs: List[CVector], outputs: List[CVector])

  def transpileMap[T, U](expr: Expr[T => U]): CompiledVeFunction = {
    // Reformat and type-annotate the tree
    val func = FunctionReformatter.reformatFunction(expr)
    val signature = func.veSignature
    val code = evalMapFunc(func)

    // Assemble and compile the function
    CompiledVeFunction(
      new CFunction2(
        s"map_${Math.abs(code.hashCode)}",
        signature.inputs.map(PointerPointer) ++ signature.outputs.map(PointerPointer),
        code,
        DefaultHeaders
      ),
      signature.outputs,
      FunctionTyping.fromExpression(expr)
    )
  }

  def sparkTypeToVeType(t: Type): VeType = Map(
    typeOf[Int] -> SparkExpressionToCExpression.sparkTypeToVeType(IntegerType),
    typeOf[Long] -> SparkExpressionToCExpression.sparkTypeToVeType(LongType),
    typeOf[Float] -> SparkExpressionToCExpression.sparkTypeToVeType(FloatType),
    typeOf[Double] -> SparkExpressionToCExpression.sparkTypeToVeType(DoubleType),
    typeOf[Instant] -> SparkExpressionToCExpression.sparkTypeToVeType(LongType),
  )(t)

  def evalGroupBy(fun: Function, signature: VeSignature): String = {
    fun.body match {
      case ident @ Ident(name) => CodeLines.from(
        s"${evalIdent(ident)};",
      ).indented.cCode
      case apply @ Apply(fun, args) => groupByCode(signature, evalApply(apply, signature))
      case select @ Select(tree, name) => groupByCode(signature, evalSelect(select))
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
    } else if (t =:= typeOf[Double]) {
      "nullable_double_vector"
    } else {
      throw new IllegalArgumentException(s"Invalid resultTypeForType $t")
    }
  }

  private def groupByCode(signature: VeSignature, predicate_code: String) = {
    val resultType: String = signature.outputs.head.veType.cVectorType

    val debug = false
    val mark = (m: String) => debugPrint(m,debug)
    val sout = (m: String, v: String) => debugPrint(m,v,debug)
    val soutv = (v: String) => sout(s"$v = ", v)

    val inputsWithIdx = signature.inputs.zipWithIndex
    CodeLines.from(
      // Merge inputs and assign output to pointer
      inputsWithIdx.map{case (input, idx) => CodeLines.from(
        s"${input.veType.cVectorType} *tmp_$idx = ${input.veType.cVectorType}::merge(${input.name}, input_batch_count);",
      )},
      s"size_t len = tmp_0->count;",
      soutv("len"),
      s"std::vector<size_t> grouping(len);",
      s"std::vector<size_t> grouping_keys;",
      inputsWithIdx.map{case (input, idx) => CodeLines.from(
        s"${input.veType.cScalarType} ${input.name}_val {};",
      )},
      mark("grouping"),
      s"for (auto i = 0; i < len; i++) {",
      CodeLines.from(
        inputsWithIdx.map{case (input, idx) => CodeLines.from(
            s"${input.name}_val = tmp_$idx->data[i];",
        )},
        s"grouping[i] = ${predicate_code};",
        //soutv("bitmask[i]"),
      ).indented,
      s"}",
      debugPrint("separate_to_groups", debug),
      s"std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, grouping_keys);",
      s"""auto group_count = grouping_keys.size();""",
      s"""*groups_out = ${resultType}::allocate();""",
      s"""groups_out[0]->resize(group_count);""",
      CodeLines.forLoop("i", "group_count"){
        CodeLines.from(
          "groups_out[0]->data[i] = grouping_keys[i];",
          "groups_out[0]->set_validity(i, 1);"
        )
      },
      s"""//cyclone::print_vec("grouping_keys", grouping_keys);""",
      mark("malloc(group_count)"),
      inputsWithIdx.map{case (input, idx) => CodeLines.from(
        s"*out_$idx = static_cast<${resultType} **>(malloc(sizeof(nullptr) * group_count));",
      )},
      inputsWithIdx.map{case (input, idx) => CodeLines.forLoop("i", "group_count"){
          CodeLines.from(
            //s"""std::cout << "tmp->select(groups[i]) (" << i << ")" << std::endl;""",
            s"out_$idx[0][i] = tmp_$idx->select(groups[i]);"
          )
        }
      },
      inputsWithIdx.map{case (input, idx) => CodeLines.from(
        s"free(tmp_$idx);",
      )},
      mark("done")
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
      case apply @ Apply(fun, args) => filterCode(defs, evalApply(apply, null), t)
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
            } else if (t =:= typeOf[Double]) {
              "double"
            } else {
              throw new IllegalArgumentException(s"Invalid evalScalarType $t")
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
        } else if (t =:= typeOf[Double]) {
          "double"
        } else {
          throw new IllegalArgumentException(s"Invalid evalScalarType $t")
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
  def evalMapFunc(func: Function): String = {
    val signature = func.veSignature

    val codelines = func.body match {
      case ident @ Ident(name) =>
        CodeLines.from(s"${signature.outputs.head.name}[0] = ${evalIdent(ident).replace("_val", "")}[0]->clone();")

      case apply @ Apply(fun, args) =>
        CodeLines.from(
          // Get len
          s"size_t len = ${signature.inputs.head.name}[0]->count;",
          "",
          // Allocate outvecs
          signature.outputs.map { (output: CVector) => CodeLines.from(
            s"${output.name}[0] = ${output.veType.cVectorType}::allocate();",
            s"${output.name}[0]->resize(len);"
          )},
          "",
          // Declare tmp vars
          signature.inputs.map { d =>
            s"${d.veType.cScalarType} ${d.name}_val {};"
          },
          "",
          // Loop over all rows
          CodeLines.forLoop("i", "len") {
            CodeLines.from(
              // Fetch values
              signature.inputs.map { d => s"${d.name}_val = ${d.name}[0]->data[i];" },
              "",
              // Perform map operation
              (apply.fun, apply.args) match {
                case (TypeApply(Select(Ident(ident), TermName("apply")), _), args2) if ident.toString.startsWith("Tuple") =>
                  CodeLines.from(args2.zip(signature.outputs).map { case (tupleArg, outputArg) =>
                    s"${outputArg.name}[0]->data[i] = ${evalArg(tupleArg)};"
                  })

                case _ =>
                  s"out_0[0]->data[i] = ${evalApply(apply, signature)};"
              },
              "",
              // Set validity
              signature.outputs.map { d => s"${d.name}[0]->set_validity(i, 1);" }
            )
          }
        )

      case Literal(Constant(value)) =>
        CodeLines.from(
          // Get len
          s"size_t len = ${signature.inputs.head.name}[0]->count;",
          "",
          // Allocate outvecs
          signature.outputs.map { (output: CVector) => CodeLines.from(
            s"${output.name}[0] = ${output.veType.cVectorType}::allocate();",
            s"${output.name}[0]->resize(len);"
          )},
          "",
          CodeLines.forLoop("i", "len") {
            // Set value and validity
            CodeLines.from(
              signature.outputs.map { d => s"${d.name}[0]->data(i] = ${value};" },
              signature.outputs.map { d => s"${d.name}[0]->set_validity(i, 1);" }
            )
          }
        )

      case unknown =>
        CodeLines.from(showRaw(unknown))
    }

    codelines.indented.cCode
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
      case apply @ Apply(_) => evalApply(apply, null)
      case unknown => "<unknown args in apply: " + showRaw(unknown) + ">"
    }
  }

  def evalApply(apply: Apply, veSignature: VeSignature): String = {
    PartialFunction.condOpt(apply.fun) {
      /*
        Handle the case of calling static methods of `java.time.Instant` to
        instantiate a timestamp by evaluating them in real-time, e.g. `Instant.now`
      */
      case Select(y, _) if Option(y.symbol).map(_.fullName).contains(classOf[java.time.Instant].getName) =>
        toolbox.eval(toolbox.untypecheck(apply))
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
      case apply @ Apply(_) => evalApply(apply, null)
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
      case apply @ Apply(x) => evalApply(apply, null)
      case lit @ Literal(_) => evalLiteral(lit)
      case unknown => "<unknown qual: " + showRaw(unknown) + ">"
    }

  }
  def evalQualScalar(tree: Tree): String = {
    tree match {
      case ident @ Ident(_) => evalIdent(ident)
      case apply @ Apply(x) => evalApply(apply, null)
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

  private def debugPrint(marker: String, expr: String, enabled: Boolean): CodeLines = {
    if(enabled){
      s"""std::cout << "${marker.replaceAllLiterally("\"", "\\\"")}" << $expr << std::endl;"""
    }else{
      ""
    }
  }

  private def debugPrint(marker: String, enabled: Boolean): CodeLines = {
    if(enabled){
      s"""std::cout << "${marker.replaceAllLiterally("\"", "\\\"")}" << std::endl;"""
    }else{
      ""
    }
  }
}
