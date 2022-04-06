package com.nec.native

import com.nec.native.SyntaxTreeOps._
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.{PointerPointer, PointerPointerPointer, Raw}
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector, CodeLines}
import com.nec.util.DateTimeOps._

import java.time.Instant
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object CppTranspiler {
  private val toolbox = CompilerToolBox.get

  def transpileGroupBy[T, K](expr: universe.Expr[T => K]): CompiledVeFunction = {
    val func = FunctionReformatter.reformatFunction(expr)
    val mapSignature = func.veMapSignature
    val signature = func.veInOutSignature
    require(mapSignature.outputs.size == 1, s"Can not group by anything other than simple types! Got: ${signature.outputs}")

    val keyType = mapSignature.outputs.head

    val code = func match {
      case fun @ Function(vparams, body) => evalGroupBy(fun, signature)
    }

    val funcName = s"groupby_${Math.abs(code.hashCode())}"

    CompiledVeFunction(new CFunction2(
      funcName,
      Seq(
        Raw("size_t input_batch_count"),
        PointerPointer(CVector("groups_out", keyType.veType)),
      ) ++ signature.inputs.map(PointerPointer) ++ signature.outputs.map(PointerPointerPointer),
      code,
      DefaultHeaders
    ),
      // Outputs are for the groups
      signature.outputs,
      // Typing is for the Keying function
      FunctionTyping.fromExpression(expr))
  }

  def transpileReduce[T](expr: universe.Expr[(T, T) => T]): CompiledVeFunction = {
    val func = FunctionReformatter.reformatFunction(expr)
    val signature = func.veReduceSignature
    val code = evalReduceFunc(func)

    CompiledVeFunction(
      new CFunction2(
        s"reduce_${Math.abs(code.hashCode)}",
        signature.inputs.map(PointerPointer) ++ signature.outputs.map(PointerPointer),
        code,
        DefaultHeaders
      ),
      signature.outputs,
      FunctionTyping.fromExpression(expr)
    )
  }

  def transpileFilter[T](expr: universe.Expr[T => Boolean]): CompiledVeFunction = {
    val func = FunctionReformatter.reformatFunction(expr)
    val signature = func.veInOutSignature
    val code = evalFilterFunc(func)

    CompiledVeFunction(
      new CFunction2(
        s"filter_${Math.abs(code.hashCode)}",
        signature.inputs.map(PointerPointer) ++ signature.outputs.map(PointerPointer),
        code,
        DefaultHeaders
      ),
      signature.outputs,
      FunctionTyping.fromExpression(expr)
    )
  }


  def transpileMap[T, U](expr: Expr[T => U]): CompiledVeFunction = {
    // Reformat and type-annotate the tree
    val func = FunctionReformatter.reformatFunction(expr)
    val signature = func.veMapSignature
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

  def evalGroupBy(fun: Function, signature: VeSignature): String = {
    fun.body match {
      case ident @ Ident(TermName(name)) => groupByCode(signature, name)
      case apply @ Apply(fun, args) => groupByCode(signature, evalApply(apply))
      case select @ Select(tree, name) => groupByCode(signature, evalSelect(select))
      case unknown => showRaw(unknown)
    }
  }

  def evalReduceFunc(func: Function): String = {
    val signature = func.veReduceSignature
    val aggs = func.aggregateParams
    val codelines = func.body match {
      case ident @ Ident(name) =>
        CodeLines.from(
          s"size_t len = ${signature.inputs.head.name}[0]->count;",
          // Allocate output
          signature.outputs.map { out => CodeLines.from(
            s"${out.name}[0] = ${out.veType.cVectorType}::allocate();",
            s"${out.name}[0]->resize(1);",
          )},
          // Set last element of inputs
          "",
          signature.outputs.zip(signature.inputs).map { case (out, in) => CodeLines.from(
            s"${out.name}[0]->data[0] = ${in.name}[0]->data[len - 1];",
            s"${out.name}[0]->set_validity(0, 1);",
          )}
        )

      case apply @ Apply(_, _) =>
        CodeLines.from(
          s"size_t len = ${signature.inputs.head.name}[0]->count;",
          // Allocate output
          signature.outputs.map { out => CodeLines.from(
            s"${out.name}[0] = ${out.veType.cVectorType}::allocate();",
            s"${out.name}[0]->resize(1);",
          )},
          "",
          // Allocate tmp input values
          signature.inputs.map { in => s"${in.veType.cScalarType} ${in.name}_val {};" },
          // Allocate tmp aggregate values
          aggs.map{ vdef => s"${vdef.tpt.tpe.toVeType.cScalarType} ${vdef.name} {};" },
          "",
          // Loop over values
          CodeLines.forLoop("i", "len") {
            CodeLines.from(
              // Fetch input values
              signature.inputs.map { in => s"${in.name}_val = ${in.name}[0]->data[i];" },
              "",
              // Perform the reduction
              (apply.fun, apply.args) match {
                case (TypeApply(Select(Ident(ident), TermName("apply")), _), args2) if ident.toString.startsWith("Tuple") =>
                  args2.zip(aggs).map { case (tupleArg, agg) => s"${agg.name} = ${evalQual(tupleArg)};" }
                case _ =>
                  List(s"${aggs.head.name} = ${evalApply(apply)};")
              },
            ),
          },
          "",
          signature.outputs.zip(aggs).map { case (out, agg) => CodeLines.from(
            s"${out.name}[0]->data[0] = ${agg.name};",
            s"${out.name}[0]->set_validity(0, 1);",
          )}
        )

      case unknown =>
        CodeLines.from(showRaw(unknown))
    }

    codelines.indented.cCode
  }

  private def groupByCode(signature: VeSignature, predicateCode: String) = {
    val resultType: String = signature.outputs.head.veType.cVectorType

    val debug = false
    val mark = (m: String) => debugPrint(m,debug)
    val sout = (m: String, v: String) => debugPrint(m,v,debug)
    val soutv = (v: String) => sout(s"$v = ", v)

    CodeLines.from(
      // Merge inputs and assign output to pointer
      signature.inputs.map{ input => CodeLines.from(
        s"${input.veType.cVectorType} *tmp_${input.name} = ${input.veType.cVectorType}::merge(${input.name}, input_batch_count);",
      )},
      s"size_t len = tmp_${signature.inputs.head.name}->count;",
      soutv("len"),
      s"std::vector<size_t> grouping(len);",
      s"std::vector<size_t> grouping_keys;",
      signature.inputs.map{ input => CodeLines.from(
        s"${input.veType.cScalarType} ${input.name}_val {};",
      )},
      mark("grouping"),
      s"for (auto i = 0; i < len; i++) {",
      CodeLines.from(
        signature.inputs.map{ input => CodeLines.from(
            s"${input.name}_val = tmp_${input.name}->data[i];",
        )},
        s"grouping[i] = ${predicateCode};",
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
      s"""// cyclone::print_vec("grouping_keys", grouping_keys);""",
      mark("malloc(group_count)"),
      signature.outputs.map{ output => CodeLines.from(
        s"*${output.name} = static_cast<${resultType} **>(malloc(sizeof(nullptr) * group_count));",
      )},
      signature.outputs.zip(signature.inputs).map{case (out, in) => CodeLines.forLoop("i", "group_count"){
          CodeLines.from(
            //s"""std::cout << "tmp->select(groups[i]) (" << i << ")" << std::endl;""",
            s"${out.name}[0][i] = tmp_${in.name}->select(groups[i]);"
          )
        }
      },
      signature.inputs.map{ input => CodeLines.from(
        s"free(tmp_${input.name});",
      )},
      mark("done")
    ).indented.cCode
  }

  private def filterCode(signature: VeSignature, template: (CodeLines, String)): CodeLines = {
    val (blockLines, predicateCode) = template
    CodeLines.from(
      s"size_t len = ${signature.inputs.head.name}[0]->count;",
      s"std::vector<size_t> bitmask(len);",
      "",
      signature.inputs.map { in => s"${in.veType.cScalarType} ${in.name}_val {};" },
      "",
      CodeLines.forLoop("i", "len") {
        CodeLines.from(
          signature.inputs.map { in => s"${in.name}_val = ${in.name}[0]->data[i];" },
          "",
          blockLines,
          "",
          s"bitmask[i] = ${predicateCode};"
        )
      },
      "",
      s"auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);",
      signature.outputs.zip(signature.inputs).map { case (out, in) =>
        s"${out.name}[0] = ${in.name}[0]->select(matching_ids);",
      }
    )
  }

  private def filterOperation(block: Block): (CodeLines, String) = {
    /*
      The last line of the code block is the only line that is transpiled
      differently because its return value(s) need to be written into the output
      vecs.
    */
    val lastline = block.children.last match {
      case apply: Apply =>
        evalApply(apply)

      case select: Select =>
        evalSelect(select)

      case ifblock: If =>
        evalIf(ifblock)
    }

    (
      CodeLines.from(
        // Only the first N-1 lines of the block are straightforwardly transpiled
        block.children.dropRight(1)
          .map(evalQual)
          .filter(_.nonEmpty)
          .map(l => s"${l};"),
      ),
      lastline
    )
  }

  def evalFilterFunc(func: Function): String = {
    val signature = func.veInOutSignature
    val codelines = func.body match {
      case ident: Ident =>
        CodeLines.from(s"${signature.outputs.head.name}[0] = ${evalIdent(ident).replace("_val", "")}[0]->clone();")

      case apply: Apply =>
        // To reduce code duplication, shove the Apply into a Block and eval as a block
        filterCode(signature, filterOperation(Block(q"()", apply)))

      case select: Select =>
        // To reduce code duplication, shove the Select into a Block and eval as a block
        filterCode(signature, filterOperation(Block(q"()", select)))

      case ifblock: If =>
        // To reduce code duplication, shove the If into a Block and eval as a block
        filterCode(signature, filterOperation(Block(q"()", ifblock)))

      case Literal(Constant(true)) | Block(_, Literal(Constant(true))) =>
        // If the body is a singular `true` value or a block with a constant `true` return value, ignore all other computations inside the block and clone
        CodeLines.from(signature.inputs.zip(signature.outputs).map { case (input, output) =>
          s"${output.name}[0] = ${input.name}->clone();"
        })

      case Literal(Constant(false)) | Block(_, Literal(Constant(false))) =>
        // If the body is a singular `false` value or a block with a constant `false` return value, ignore all other computations inside the block and allocate empty vec
        CodeLines.from(signature.outputs.map { output => CodeLines.from(
          s"${output.name}[0] = ${output.veType.cVectorType}::allocate();",
          s"${output.name}[0]->resize(0);",
        )})

      case block: Block =>
        filterCode(signature, filterOperation(block))

      case unknown =>
        CodeLines.from(showRaw(unknown))
    }

    codelines.indented.cCode
  }

  private def mapCode(signature: VeSignature, mapOperation: CodeLines): CodeLines = {
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
      signature.inputs.map { in => s"${in.veType.cScalarType} ${in.name}_val {};" },
      "",
      // Loop over all rows
      CodeLines.forLoop("i", "len") {
        CodeLines.from(
          // Fetch values
          signature.inputs.map { in => s"${in.name}_val = ${in.name}[0]->data[i];" },
          "",
          // Perform map operation and set validity
          mapOperation,
          signature.outputs.map { d => s"${d.name}[0]->set_validity(i, 1);" }
        )
      }
    )
  }

  private def mapOperation(block: Block, signature: VeSignature): CodeLines = {
    /*
      The last line of the code block is the only line that is transpiled
      differently because its return value(s) need to be written into the output
      vecs.
    */
    val lastline = block.children.last match {
      case ident @ Ident(name) =>
        CodeLines.from(s"${signature.outputs.head.name}[0]->data[i] = ${evalIdent(ident)};")

      case apply: Apply =>
        CodeLines.from {
          (apply.fun, apply.args) match {
            case (TypeApply(Select(Ident(ident), TermName("apply")), _), args) if ident.toString.startsWith("Tuple") =>
              args.zip(signature.outputs).map { case (tupleArg, out) =>
                s"${out.name}[0]->data[i] = ${evalQual(tupleArg)};"
              }

            case _ =>
              List(s"out_0[0]->data[i] = ${evalApply(apply)};")
          }
        }

      case ifblock: If =>
        CodeLines.from(s"out_0[0]->data[i] = ${evalIf(ifblock)};")

      case select: Select =>
        CodeLines.from(s"out_0[0]->data[i] = ${evalSelect(select)};")

      case Literal(Constant(value)) =>
        CodeLines.from(s"out_0[0]->data[i] = ${value};")
    }

    CodeLines.from(
      // Only the first N-1 lines of the block are straightforwardly transpiled
      block.children.dropRight(1)
        .map(evalQual)
        .filter(_.nonEmpty)
        .map(l => s"${l};"),
      "",
      lastline
    )
  }

  private def mapCodeForConstant(signature: VeSignature, value: Any): CodeLines = {
    CodeLines.from(
      s"size_t len = ${signature.inputs.head.name}[0]->count;",
      "",
      signature.outputs.map { (output: CVector) =>
        s"${output.name}[0] = ${output.veType.cVectorType}::constant(len, ${value});"
      }
    )
  }

  def evalMapFunc(func: Function): String = {
    val signature = func.veMapSignature
    val codelines = func.body match {
      case ident: Ident =>
        CodeLines.from(s"${signature.outputs.head.name}[0] = ${evalIdent(ident).replace("_val", "")}[0]->clone();")

      case apply: Apply =>
        // To reduce code duplication, shove the Apply into a Block and eval as a block
        mapCode(signature, mapOperation(Block(q"()", apply), signature))

      case select: Select =>
        // To reduce code duplication, shove the Select into a Block and eval as a block
        mapCode(signature, mapOperation(Block(q"()", select), signature))

      case ifblock: If =>
        // To reduce code duplication, shove the If into a Block and eval as a block
        mapCode(signature, mapOperation(Block(q"()", ifblock), signature))

      case Literal(Constant(value)) =>
        // If the body is a constant value, just create a constant vec
        mapCodeForConstant(signature, value)

      case Block(_, Literal(Constant(value))) =>
        // If the body is a block with a constant return value, ignore all other computations inside the block and create a constant vec
        mapCodeForConstant(signature, value)

      case block: Block =>
        mapCode(signature, mapOperation(block, signature))

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

  def evalApply(apply: Apply): String = {
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
          val left = evalQual(tree)
          val right = evalQual(arg)
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
      val argsStr = apply.args.map(evalQual).mkString(", ")

      "(" + funStr + argsStr + ")"
    }
  }

  def evalLiteral(literal: Literal): String = {
    literal.value match {
      case Constant(true) => "1"
      case Constant(false) => "0"
      case Constant(()) => ""
      case Constant(c) => c.toString
      case unknown => "<unknown args in Literal: " + showRaw(unknown) + ">"
    }
  }

  def evalSelect(select: Select): String = {
    select.name match {
      case TermName("unary_$bang") if select.tpe =:= typeOf[Boolean] =>
        s" !${evalQual(select.qualifier)}"

      case TermName("toInt") if select.tpe =:= typeOf[Int] =>
        s"int32_t( ${evalQual(select.qualifier)} )"

      case TermName("toShort") if select.tpe =:= typeOf[Short] =>
        // Cast to int16_t first before fitting it back to int32_t
        s"int32_t(int16_t( ${evalQual(select.qualifier)} ))"

      case TermName("toLong") if select.tpe =:= typeOf[Long] =>
        s"int64_t( ${evalQual(select.qualifier)} )"

      case TermName("toFloat") if select.tpe =:= typeOf[Float] =>
        s"float( ${evalQual(select.qualifier)} )"

      case TermName("toDouble") if select.tpe =:= typeOf[Double] =>
        s"double( ${evalQual(select.qualifier)} )"

      case TermName("toByte") if select.tpe =:= typeOf[Byte] =>
        s"int8_t( ${evalQual(select.qualifier)} )"

      case TermName("toChar") if select.tpe =:= typeOf[Char] =>
        s"char16_t( ${evalQual(select.qualifier)} )"

      case _ =>
        s"${evalQual(select.qualifier)}${evalName(select.name)}"
    }
  }

  def evalQual(tree: Tree): String = {
    tree match {
      case x: Ident   => evalIdent(x)
      case x: Apply   => evalApply(x)
      case x: Literal => evalLiteral(x)
      case x: Select  => evalSelect(x)
      case x: ValDef  => evalValDef(x)
      case x: Assign  => evalAssign(x)
      case x: Block   => evalBlock(x)
      case x: If      => evalIf(x)
      case unknown => "<unknown qual: " + showRaw(unknown) + ">"
    }
  }

  def evalBlock(block: Block): String = {
    val lines = block.children
      .map(evalQual)
      .filter(_.nonEmpty)
      .map(l => s"${l};")

    CodeLines.from(
      "({",
      CodeLines.from(lines).indented,
      "})"
    ).cCode
  }

  def evalIf(ifblock: If): String = {
    s"( ${evalQual(ifblock.cond)} ) ? ( ${evalQual(ifblock.thenp)} ) : ( ${evalQual(ifblock.elsep)} )"
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
      case unknown => s"[[ <UNKNOWN> in evalName: ${unknown} ]]"
    }
  }

  def evalValDef(vdef: ValDef): String = {
    val modifier = if (vdef.mods.hasFlag(Flag.MUTABLE)) "" else "const ";
    s"${modifier}${vdef.tpt.tpe.toVeType.cScalarType} ${vdef.name} = ${evalQual(vdef.rhs)}"
  }

  def evalAssign(assign: Assign): String = {
    s"${evalQual(assign.lhs)} = ${evalQual(assign.rhs)}"
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
