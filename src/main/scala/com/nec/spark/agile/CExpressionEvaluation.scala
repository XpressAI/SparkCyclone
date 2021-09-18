package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.types._
import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}

object CExpressionEvaluation {
  def cType(d: DataType): String = {
    d match {
      case DoubleType =>
        s"double"
      case IntegerType | DateType =>
        s"int"
      case LongType =>
        s"long"
      case StringType =>
        s"char"
      case x =>
        sys.error(s"unsupported dataType $x")
    }
  }

  def veType(d: DataType): CFunctionGeneration.VeType = {
    d match {
      case DoubleType =>
        CFunctionGeneration.VeType.VeNullableDouble
      case IntegerType | DateType =>
        CFunctionGeneration.VeType.VeNullableInt
      case x =>
        sys.error(s"unsupported dataType $x")
    }
  }

  def cSize(d: DataType): Int = {
    Map[DataType, Int](DoubleType -> 8, IntegerType -> 4, DateType -> 4, LongType -> 8)
      .getOrElse(d, sys.error(s"unsupported dataType $d"))
  }

  def indexForInput(inputs: Seq[Attribute], expression: Expression): Int = {
    expression match {
      case AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.name == name) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx =>
            idx
        }
      case _ => -1
    }
  }

  def dataTypeOfSub(inputs: Seq[Attribute], expression: Expression): DataType = {
    expression match {
      case e: AttributeReference =>
        inputs(indexForInput(inputs, expression)).dataType
      case Subtract(left, right, _) =>
        val leftType = dataTypeOfSub(inputs, left)
        val rightType = dataTypeOfSub(inputs, right)
        if (leftType == DoubleType || rightType == DoubleType) {
          DoubleType
        } else {
          leftType
        }
      case Multiply(left, right, _) =>
        val leftType = dataTypeOfSub(inputs, left)
        val rightType = dataTypeOfSub(inputs, right)
        if (leftType == DoubleType || rightType == DoubleType) {
          DoubleType
        } else {
          leftType
        }
      case Add(left, right, _) =>
        val leftType = dataTypeOfSub(inputs, left)
        val rightType = dataTypeOfSub(inputs, right)
        if (leftType == DoubleType || rightType == DoubleType) {
          DoubleType
        } else {
          leftType
        }
      case Divide(left, right, _) =>
        val leftType = dataTypeOfSub(inputs, left)
        val rightType = dataTypeOfSub(inputs, right)
        if (leftType == DoubleType || rightType == DoubleType) {
          DoubleType
        } else {
          leftType
        }
      case Abs(v) =>
        dataTypeOfSub(inputs, v)
      case Sum(child) =>
        dataTypeOfSub(inputs, child)
      case Average(child) =>
        dataTypeOfSub(inputs, child)
      case Min(child) =>
        dataTypeOfSub(inputs, child)
      case Max(child) =>
        dataTypeOfSub(inputs, child)
      case Corr(_, _, _) =>
        DoubleType
      case Literal(v, DoubleType) =>
        DoubleType
      case Literal(v, IntegerType) =>
        IntegerType
    }
  }

  def cTypeOfSub(inputs: Seq[Attribute], expression: Expression): String = {
    cType(dataTypeOfSub(inputs, expression))
  }

  def cGenProject(
    fName: String,
    inputReferences: Set[String],
    childOutputs: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    maybeFilter: Option[Expression]
  )(implicit nameCleaner: NameCleaner): CodeLines = {
    val inputs = {
      val attrs = childOutputs
        .filter(attr => inputReferences.contains(attr.name))

      if (childOutputs.size > attrs.size) childOutputs else attrs
    }

    val inputBits = inputs.zipWithIndex
      .map { case (i, idx) =>
        i.dataType match {
          case DoubleType =>
            s"nullable_double_vector* input_${idx}"
          case IntegerType =>
            s"nullable_int_vector* input_${idx}"
          case LongType =>
            s"nullable_bigint_vector* input_${idx}"
          case StringType =>
            s"nullable_varchar_vector* input_${idx}"
          case DateType =>
            s"nullable_int_vector* input_${idx}"
          case x =>
            sys.error(s"Invalid input dataType $x")
        }
      }

    val outputBits = resultExpressions.zipWithIndex.map { case (i, idx) =>
      i.dataType match {
        case DoubleType =>
          s"nullable_double_vector* output_${idx}"
        case IntegerType | DateType =>
          s"nullable_int_vector* output_${idx}"
        case LongType =>
          s"nullable_bigint_vector* output_${idx}"
        case StringType =>
          s"nullable_varchar_vector* output_${idx}"
        case x =>
          sys.error(s"Invalid output dataType $x")
      }
    }

    val arguments = inputBits ++ outputBits

    List[List[String]](
      List(
        "#include <cmath>",
        "#include <bitset>",
        "#include <iostream>",
        s"""extern "C" long ${fName}(${arguments.mkString(", ")})""",
        "{"
      ),
      maybeFilter.toList.flatMap(cond => filterInputs(cond, inputs)),
      resultExpressions.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"long output_${idx}_count = input_0->count;",
          s"${cType(res.dataType)} *output_${idx}_data = (${cType(res.dataType)}*) malloc(output_${idx}_count * sizeof(${cType(res.dataType)}));",
          s"unsigned char *validity_buffer_${idx} = (unsigned char*) malloc(ceil(output_${idx}_count/8.0) * sizeof(unsigned char));",
          s"std::bitset<8> validity_bitset_${idx};",
          s"int j_${idx} = 0;"
        )
      }.toList,
      List("#pragma _NEC ivdep", "for (int i = 0; i < output_0_count; i++) {"),
      resultExpressions.zipWithIndex.flatMap { case (re, idx) =>
        List(
          s"if(${genNullCheck(inputs, re)})",
          "{",
          s"output_${idx}_data[i] = ${evaluateExpression(inputs, re)};",
          s"validity_bitset_${idx}.set(i%8, true);",
          "}",
          s"else { validity_bitset_${idx}.set(i%8, false);}",
          "if(i % 8 == 7 || i == output_0_count - 1) { ",
          s"validity_buffer_${idx}[j_${idx}] = (static_cast<unsigned char>(validity_bitset_${idx}.to_ulong()));",
          s"j_${idx} += 1;",
          s"validity_bitset_${idx}.reset(); }"
        )
      }.toList,
      List("}"),
      // Set outputs
      resultExpressions.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"output_${idx}->count = output_${idx}_count;",
          s"output_${idx}->data = output_${idx}_data;",
          s"output_${idx}->validityBuffer = validity_buffer_${idx};"
        )
      }.toList,
      List("return 0;", "}")
    ).flatten.codeLines
  }

  def cGenJoin(
    fName: String,
    inputs: Seq[Attribute],
    resultExpressions: Seq[Attribute],
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId],
    leftKeyExpr: Expression,
    rightKeyExpr: Expression
  )(implicit nameCleaner: NameCleaner): CodeLines = {
    val inputBits = inputs.zipWithIndex
      .map { case (i, idx) =>
        i.dataType match {
          case DoubleType =>
            s"nullable_double_vector* input_${idx}"
          case IntegerType =>
            s"nullable_int_vector* input_${idx}"
          case LongType =>
            s"nullable_bigint_vector* input_${idx}"
          //          case StringType =>
          //            s"nullable_varchar_vector* input_${idx}" //TODO: Add proper Varchar output and possibly varchar Join column
          case x =>
            sys.error(s"Invalid input dataType $x")
        }
      }

    val outputBits = resultExpressions.zipWithIndex.map { case (i, idx) =>
      i.dataType match {
        case DoubleType =>
          s"nullable_double_vector* output_${idx}"
        case IntegerType =>
          s"nullable_int_vector* output_${idx}"
        case LongType =>
          s"nullable_bigint_vector* output_${idx}"
        //            case StringType =>
        //              s"nullable_varchar_vector* output_${idx}" //TODO: Add proper Varchar output and possibly varchar Join column
        case x =>
          sys.error(s"Invalid output dataType $x")
      }
    }

    val arguments = inputBits ++ outputBits
    List[List[String]](
      List(
        "#include \"frovedis/dataframe/join.hpp\"",
        "#include <cmath>",
        "#include <bitset>",
        "#include \"frovedis/dataframe/join.cc\"",
        s"""extern "C" long ${fName}(${arguments.mkString(", ")})""",
        "{"
      ),
      List(
        s"std::vector <${cType(leftKeyExpr.dataType)}> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <${cType(rightKeyExpr.dataType)}> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        "for(int i = 0; i < input_0->count; i++) { ",
        s"left_vec.push_back(${evaluateExpression(inputs, leftKeyExpr)});",
        "left_idx.push_back(i);",
        s"right_vec.push_back(${evaluateExpression(inputs, rightKeyExpr)});",
        "right_idx.push_back(i);",
        "}"
      ),
      List(
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        "frovedis::equi_join<double>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);"
      ),
      List("long validityBuffSize = ceil(left_out.size() / 8.0);"),
      resultExpressions.zipWithIndex.flatMap {
        case (re, idx) => {
          List(
            s"output_${idx}->data=(${cTypeOfSub(inputs, re)} *) malloc(left_out.size() * sizeof(${cTypeOfSub(inputs, re)}));",
            s"output_${idx}->validityBuffer=(unsigned char *) malloc(validityBuffSize * sizeof(unsigned char*));",
            s"std::bitset<8> validity_bitset_${idx};",
            s"int j_${idx} = 0;"
          )
        }
      }.toList,
      List("#pragma _NEC ivdep", "for (int i = 0; i < left_out.size(); i++) {"),
      resultExpressions.zipWithIndex.flatMap {
        case (re, idx) => {
          List(
            s"if(${genNullCheckJoin(inputs, re, leftExprIds, rightExprIds)}) {",
            s"validity_bitset_${idx}.set(i%8, true);",
            s"output_${idx}->data[i] = ${evaluateExpressionJoin(inputs, re, leftExprIds, rightExprIds)};",
            "} else {",
            s"validity_bitset_${idx}.set(i%8, false);",
            "}",
            "if(i % 8 == 7 || i == left_out.size() - 1) { ",
            s"output_${idx}->validityBuffer[j_${idx}] = (static_cast<unsigned char>(validity_bitset_${idx}.to_ulong()));",
            s"j_${idx} += 1;",
            s"validity_bitset_${idx}.reset(); }"
          )
        }

      }.toList,
      List("}"),
      resultExpressions.zipWithIndex.flatMap { case (re, idx) =>
        List(s"output_${idx}->count = left_out.size();")
      }.toList,
      List("return 0;", "}")
    )
  }.flatten.codeLines

  def cGenJoinOuter(
    fName: String,
    inputs: Seq[Attribute],
    joinType: JoinType,
    resultExpressions: Seq[Attribute],
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId],
    leftKeyExpr: Expression,
    rightKeyExpr: Expression
  )(implicit nameCleaner: NameCleaner): CodeLines = {
    val inputBits = inputs.zipWithIndex
      .map { case (i, idx) =>
        i.dataType match {
          case DoubleType =>
            s"nullable_double_vector* input_${idx}"
          case IntegerType =>
            s"nullable_int_vector* input_${idx}"
          case LongType =>
            s"nullable_bigint_vector* input_${idx}"
          //          case StringType =>
          //            s"nullable_varchar_vector* input_${idx}" //TODO: Add proper Varchar output and possibly varchar Join column
          case x =>
            sys.error(s"Invalid input dataType $x")
        }
      }

    val outputBits = resultExpressions.zipWithIndex.map { case (i, idx) =>
      i.dataType match {
        case DoubleType =>
          s"nullable_double_vector* output_${idx}"
        case IntegerType =>
          s"nullable_int_vector* output_${idx}"
        case LongType =>
          s"nullable_bigint_vector* output_${idx}"
        //            case StringType =>
        //              s"nullable_varchar_vector* output_${idx}" //TODO: Add proper Varchar output and possibly varchar Join column
        case x =>
          sys.error(s"Invalid output dataType $x")
      }
    }

    val arguments = inputBits ++ outputBits
    List[List[String]](
      List(
        "#include \"frovedis/dataframe/join.hpp\"",
        "#include \"tuple_hash.hpp\"",
        "#include <cmath>",
        "#include <bitset>",
        "#include <tuple>",
        "#include \"frovedis/dataframe/join.cc\"",
        s"""extern "C" long ${fName}(${arguments.mkString(", ")})""",
        "{"
      ),
      List(
        s"std::vector <std::tuple<${cType(leftKeyExpr.dataType)}, int>> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <std::tuple<${cType(rightKeyExpr.dataType)}, int>> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        s"for(int i = 0; i < ${getColumnCount(leftKeyExpr, inputs)}; i++) { ",
        s"if(${genNullCheck(inputs, leftKeyExpr)}) {",
        s"left_vec.push_back(std::tuple<${cType(leftKeyExpr.dataType)}, int>(${evaluateExpression(inputs, leftKeyExpr)}, 1));",
        "} else {",
        s"left_vec.push_back(std::tuple<${cType(leftKeyExpr.dataType)}, int>(${evaluateExpression(inputs, leftKeyExpr)}, 0));",
        "}",
        "left_idx.push_back(i);",
        "}",
        "#pragma _NEC ivdep",
        s"for(int i = 0; i < ${getColumnCount(rightKeyExpr, inputs)}; i++) { ",
        s"if(${genNullCheck(inputs, rightKeyExpr)}) {",
        s"right_vec.push_back(std::tuple<${cType(rightKeyExpr.dataType)}, int>(${evaluateExpression(inputs, rightKeyExpr)}, 1));",
        "} else {",
        s"right_vec.push_back(std::tuple<${cType(rightKeyExpr.dataType)}, int>(${evaluateExpression(inputs, rightKeyExpr)}, 0));",
        "}",
        "right_idx.push_back(i);",
        "}"
      ),
      List("std::vector<size_t> right_out;", "std::vector<size_t> left_out;"),
      joinType match {
        case LeftOuter =>
          List(
            s"std::vector<size_t> outer_idx = frovedis::outer_equi_join<std::tuple<${cType(leftKeyExpr.dataType)}, int>>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);"
          )
        case RightOuter =>
          List(
            s"std::vector<size_t> outer_idx = frovedis::outer_equi_join<std::tuple<${cType(leftKeyExpr.dataType)}, int>>(right_vec, right_idx, left_vec, left_idx, right_out, left_out);"
          )
        case _ => sys.error("Only LEFT and RIGHT OUTER joins are currently supported!")
      },
      List("long validityBuffSize = ceil((left_out.size() + outer_idx.size()) / 8.0);"),
      resultExpressions.zipWithIndex.flatMap {
        case (re, idx) => {
          List(
            s"output_${idx}->data=(${cTypeOfSub(inputs, re)} *) malloc((left_out.size() + outer_idx.size()) * sizeof(${cTypeOfSub(inputs, re)}));",
            s"output_${idx}->validityBuffer=(unsigned char *) malloc(validityBuffSize * sizeof(unsigned char*));",
            s"std::bitset<8> validity_bitset_${idx};",
            s"int j_${idx} = 0;"
          )
        }
      }.toList,
      List("#pragma _NEC ivdep", "for (int i = 0; i < left_out.size(); i++) {"),
      resultExpressions.zipWithIndex.flatMap {
        case (re, idx) => {
          List(
            s"if(${genNullCheckJoin(inputs, re, leftExprIds, rightExprIds)}) {",
            s"validity_bitset_${idx}.set(i%8, true);",
            s"output_${idx}->data[i] = ${evaluateExpressionJoin(inputs, re, leftExprIds, rightExprIds)};",
            "} else {",
            s"validity_bitset_${idx}.set(i%8, false);",
            "}",
            "if(i % 8 == 7) { ",
            s"output_${idx}->validityBuffer[j_${idx}] = (static_cast<unsigned char>(validity_bitset_${idx}.to_ulong()));",
            s"j_${idx} += 1;",
            s"validity_bitset_${idx}.reset(); }"
          )
        }

      }.toList,
      List("}"),
      List(
        "#pragma _NEC ivdep",
        "for (int i = left_out.size(); i < (left_out.size() + outer_idx.size()); i++) {",
        "int idx = i - left_out.size();"
      ),
      resultExpressions.zipWithIndex.flatMap {
        case (re, idx) => {
          List(
            s"if(${genNullCheckJoinOuter(inputs, joinType, re, leftExprIds, rightExprIds)}) {",
            s"validity_bitset_${idx}.set(i%8, true);",
            s"output_${idx}->data[i] = ${evaluateExpressionJoinOuter(inputs, re, leftExprIds, rightExprIds)};",
            "} else {",
            s"validity_bitset_${idx}.set(i%8, false);",
            "}",
            "if(i % 8 == 7 || i == (left_out.size() + outer_idx.size() - 1)) { ",
            s"output_${idx}->validityBuffer[j_${idx}] = (static_cast<unsigned char>(validity_bitset_${idx}.to_ulong()));",
            s"j_${idx} += 1;",
            s"validity_bitset_${idx}.reset(); }"
          )
        }

      }.toList,
      List("}"),
      resultExpressions.zipWithIndex.flatMap { case (re, idx) =>
        List(s"output_${idx}->count = left_out.size() + outer_idx.size();")
      }.toList,
      List("return 0;", "}")
    )
  }.flatten.codeLines

  def getColumnCount(expr: Expression, input: Seq[Attribute]): String = {
    expr match {
      case NormalizeNaNAndZero(child)          => getColumnCount(child, input)
      case KnownFloatingPointNormalized(child) => getColumnCount(child, input)
      case alias @ Alias(expr, name)           => getColumnCount(alias.child, input)
      case expr @ NamedExpression(name, DoubleType | IntegerType) =>
        input.indexWhere(_.exprId == expr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expr}' from set of: ${input}")
          case idx => s"input_${idx}->count"
        }
    }
  }

  def cGenSort(fName: String, inputs: Seq[Attribute], sortingColumn: AttributeReference)(implicit
    nameCleaner: NameCleaner
  ): CodeLines = {

    val inputBits = inputs.zipWithIndex
      .map { case (i, idx) =>
        s"nullable_double_vector* input_${idx}"
      }

    val outputBits = inputs.zipWithIndex.map { case (i, idx) =>
      s"nullable_double_vector* output_${idx}"
    }

    val sortingIndex = inputs.indexWhere(att => att.name == sortingColumn.name)

    val arguments = inputBits ++ outputBits

    List[List[String]](
      List("#include \"frovedis/core/radix_sort.hpp\"", "#include <tuple>", "#include <bitset>"),
      List(s"""extern "C" long ${fName}(${arguments.mkString(", ")})""", "{"),
      List(
        s"std::tuple<int, int>* sort_column_validity_buffer = (std::tuple<int, int> *) malloc(input_${sortingIndex}->count * sizeof(std::tuple<int, double>));"
      ),
      inputs.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"long output_${idx}_count = input_0->count;",
          s"double *output_${idx}_data = (double*) malloc(output_${idx}_count * sizeof(double));",
          s"unsigned char *validity_buffer_${idx} = (unsigned char*) malloc(ceil(output_${idx}_count/8.0) * sizeof(unsigned char));",
          s"std::bitset<8> validity_bitset_${idx};",
          s"int j_${idx} = 0;"
        )
      }.toList,
      List(
        s"for(int i = 0; i < input_${sortingIndex}->count; i++)",
        "{",
        s"sort_column_validity_buffer[i] = std::tuple<int, double>{((input_${sortingIndex}->validityBuffer[i/8] >> i % 8) & 0x1), i};",
        "}"
      ),
      List(
        s"frovedis::radix_sort(input_${sortingIndex}->data, sort_column_validity_buffer, input_${sortingIndex}->count);"
      ),
      List("#pragma _NEC ivdep", "for (int i = 0; i < output_0_count; i++) {"),
      inputs.zipWithIndex.flatMap { case (re, idx) =>
        List(
          s"if(${genNullCheckSorted(inputs, re, sortingIndex)}) {",
          s"validity_bitset_${idx}.set(i%8, true);",
          s"output_${idx}_data[i] = ${if (idx != sortingIndex) evaluateExpressionSorted(inputs, re)
          else evaluateExpression(inputs, re)};",
          "} else {",
          s"validity_bitset_${idx}.set(i%8, false);}",
          "if(i % 8 == 7 || i == output_0_count - 1) { ",
          s"validity_buffer_${idx}[j_${idx}] = (static_cast<unsigned char>(validity_bitset_${idx}.to_ulong()));",
          s"j_${idx} += 1;",
          s"validity_bitset_${idx}.reset(); }"
        )
      }.toList,
      List("}"),
      // Set inputs
      inputs.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"output_${idx}->count = output_${idx}_count;",
          s"output_${idx}->data = output_${idx}_data;",
          s"output_${idx}->validityBuffer = validity_buffer_${idx};"
        )
      }.toList,
      List("return 0;", "}")
    ).flatten.codeLines
  }

  final case class AggregateDescription(
    init: List[String],
    iter: List[String],
    result: List[String],
    outputArguments: List[String]
  )

  def evaluateExpression(input: Seq[Attribute], expression: Expression): String = {
    expression match {
      case NormalizeNaNAndZero(child)          => evaluateExpression(input, child)
      case KnownFloatingPointNormalized(child) => evaluateExpression(input, child)
      case alias @ Alias(expr, name)           => evaluateSub(input, alias.child)
      case expr @ NamedExpression(name, DoubleType | IntegerType) =>
        input.indexWhere(_.exprId == expr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx => s"input_${idx}->data[i]"
        }
      case AttributeReference(name, typeName, _, _) =>
        (input.indexWhere(_.name == name), typeName) match {
          case (-1, typeName) =>
            sys.error(
              s"Could not find a reference for '${expression}' with type: ${typeName} from set of: ${input}"
            )
          case (idx, (DoubleType | IntegerType | LongType)) =>
            s"input_${idx}->data[i]"
          case (idx, actualType) => sys.error(s"'${expression}' has unsupported type: ${typeName}")
        }
    }
  }

  def evaluateExpressionSorted(input: Seq[Attribute], expression: Expression): String = {
    expression match {
      case alias @ Alias(expr, name) => evaluateSubSorted(input, alias.child)
      case AttributeReference(name, typeName, _, _) =>
        (input.indexWhere(_.name == name), typeName) match {
          case (-1, typeName) =>
            sys.error(
              s"Could not find a reference for '${expression}' with type: ${typeName} from set of: ${input}"
            )
          case (idx, (DoubleType | IntegerType | LongType)) =>
            s"input_${idx}->data[std::get<1>(sort_column_validity_buffer[i])]"
          case (idx, actualType) => sys.error(s"'${expression}' has unsupported type: ${typeName}")
        }
      case NamedExpression(name, DoubleType | IntegerType | LongType) =>
        input.indexWhere(_.name == name) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx => s"input_${idx}->data[std::get<1>(sort_column_validity_buffer[i])]"
        }
    }
  }

  def evaluateExpressionJoin(
    input: Seq[Attribute],
    expression: Expression,
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId]
  ): String = {
    expression match {
      case alias @ Alias(expr, name) =>
        evaluateSubJoin(input, alias.child, leftExprIds, rightExprIds)
      case attr @ AttributeReference(name, typeName, _, _) =>
        (input.indexWhere(_.exprId == attr.exprId), typeName) match {
          case (-1, typeName) =>
            sys.error(
              s"Could not find a reference for '${expression}' with type: ${typeName} from set of: ${input}"
            )
          case (idx, (DoubleType | IntegerType | LongType))
              if (leftExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[left_out[i]]"
          case (idx, (DoubleType | IntegerType | LongType))
              if (rightExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[right_out[i]]"

          case (idx, actualType) => sys.error(s"'${expression}' has unsupported type: ${typeName}")
        }
      case expr @ NamedExpression(name, DoubleType | IntegerType | LongType) =>
        input.indexWhere(_.exprId == expr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx if (leftExprIds.contains(expr.exprId))  => s"input_${idx}->data[left_out[i]]"
          case idx if (rightExprIds.contains(expr.exprId)) => s"input_${idx}->data[right_out[i]]"

        }
    }
  }

  def evaluateExpressionJoinOuter(
    input: Seq[Attribute],
    expression: Expression,
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId]
  ): String = {
    expression match {
      case alias @ Alias(expr, name) =>
        evaluateSubJoinOuter(input, alias.child, leftExprIds, rightExprIds)
      case attr @ AttributeReference(name, typeName, _, _) =>
        (input.indexWhere(_.exprId == attr.exprId), typeName) match {
          case (-1, typeName) =>
            sys.error(
              s"Could not find a reference for '${expression}' with type: ${typeName} from set of: ${input}"
            )
          case (idx, (DoubleType | IntegerType | LongType))
              if (leftExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[outer_idx[idx]]"
          case (idx, (DoubleType | IntegerType | LongType))
              if (rightExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[outer_idx[idx]]"

          case (idx, actualType) => sys.error(s"'${expression}' has unsupported type: ${typeName}")
        }
      case expr @ NamedExpression(name, DoubleType | IntegerType | LongType) =>
        input.indexWhere(_.exprId == expr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx if (leftExprIds.contains(expr.exprId))  => s"input_${idx}->data[left_out[idx]]"
          case idx if (rightExprIds.contains(expr.exprId)) => s"input_${idx}->data[right_out[idx]]"

        }
    }
  }

  def genNullCheck(inputs: Seq[Attribute], expression: Expression): String = {

    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx =>
            s"((input_${idx}->validityBuffer[i/8] >> i % 8) & 0x1) == 1"
        }
      case NormalizeNaNAndZero(child) => genNullCheck(inputs, child)

      case KnownFloatingPointNormalized(child) => genNullCheck(inputs, child)
      case alias @ Alias(expr, name)           => genNullCheck(inputs, alias.child)
      case DateSub(startDate, days) =>
        s"${genNullCheck(inputs, startDate)} && ${genNullCheck(inputs, days)}"
      case DateAdd(startDate, days) =>
        s"${genNullCheck(inputs, startDate)} && ${genNullCheck(inputs, days)}"
      case Subtract(left, right, _) =>
        s"${genNullCheck(inputs, left)} && ${genNullCheck(inputs, right)}"
      case Multiply(left, right, _) =>
        s"${genNullCheck(inputs, left)} && ${genNullCheck(inputs, right)}"
      case Add(left, right, _) =>
        s"${genNullCheck(inputs, left)} &&  ${genNullCheck(inputs, right)}"
      case Divide(left, right, _) =>
        s"${genNullCheck(inputs, left)} && ${genNullCheck(inputs, right)}"
      case Abs(v) =>
        s"${genNullCheck(inputs, v)}"
      case Literal(v, DoubleType | IntegerType) =>
        "true"
    }
  }

  def genNullCheckSorted(
    inputs: Seq[Attribute],
    expression: Expression,
    sortingColumnIndex: Int
  ): String = {

    expression match {
      case AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.name == name) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx if idx == sortingColumnIndex =>
            s"std::get<0>(sort_column_validity_buffer[i]) == 1"
          case idx =>
            s"(input_${idx}->validityBuffer[std::get<1>(sort_column_validity_buffer[i])/8] >> (std::get<1>(sort_column_validity_buffer[i]) % 8) & 0x1) == 1"
        }
      case alias @ Alias(expr, name) => genNullCheckSorted(inputs, alias.child, sortingColumnIndex)
      case Subtract(left, right, _) =>
        s"${genNullCheckSorted(inputs, left, sortingColumnIndex)} && ${genNullCheckSorted(inputs, right, sortingColumnIndex)}"
      case Multiply(left, right, _) =>
        s"${genNullCheckSorted(inputs, left, sortingColumnIndex)} && ${genNullCheckSorted(inputs, right, sortingColumnIndex)}"
      case Add(left, right, _) =>
        s"${genNullCheckSorted(inputs, left, sortingColumnIndex)} && ${genNullCheckSorted(inputs, right, sortingColumnIndex)}"
      case Divide(left, right, _) =>
        s"${genNullCheckSorted(inputs, left, sortingColumnIndex)} && ${genNullCheckSorted(inputs, right, sortingColumnIndex)}"
      case Abs(v) =>
        s"${genNullCheckSorted(inputs, v, sortingColumnIndex)}"
      case Literal(v, DoubleType | IntegerType) =>
        "true"
    }
  }

  def genNullCheckJoin(
    inputs: Seq[Attribute],
    expression: Expression,
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId]
  ): String = {

    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx if (leftExprIds.contains(attr.exprId)) =>
            s"((input_${idx}->validityBuffer[left_out[i]/8] >> left_out[i] % 8) & 0x1) == 1"
          case idx if (rightExprIds.contains(attr.exprId)) =>
            s"((input_${idx}->validityBuffer[right_out[i]/8] >> right_out[i] % 8) & 0x1) == 1"
        }
      case alias @ Alias(expr, name) =>
        genNullCheckJoin(inputs, alias.child, leftExprIds, rightExprIds)
      case Subtract(left, right, _) =>
        s"${genNullCheckJoin(inputs, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Multiply(left, right, _) =>
        s"${genNullCheckJoin(inputs, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Add(left, right, _) =>
        s"${genNullCheckJoin(inputs, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Divide(left, right, _) =>
        s"${genNullCheckJoin(inputs, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Abs(v) =>
        s"${genNullCheckJoin(inputs, v, leftExprIds, rightExprIds)}"
      case Literal(v, DoubleType | IntegerType) =>
        "true"
    }
  }

  def genNullCheckJoinOuter(
    inputs: Seq[Attribute],
    joinType: JoinType,
    expression: Expression,
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId]
  ): String = {
    val outerJoinIds = joinType match {
      case RightOuter => rightExprIds
      case LeftOuter  => leftExprIds
      case _          => sys.error("Only LEFT JOIN and RIGHT JOIN are currently supported.")
    }

    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx if (outerJoinIds.contains(attr.exprId)) =>
            s"((input_${idx}->validityBuffer[outer_idx[idx]/8] >> outer_idx[idx] % 8) & 0x1) == 1"
          case _ =>
            s"false" //We are using only left since this is outer join
        }
      case alias @ Alias(expr, name) =>
        genNullCheckJoinOuter(inputs, joinType, alias.child, leftExprIds, rightExprIds)
      case Subtract(left, right, _) =>
        s"${genNullCheckJoinOuter(inputs, joinType, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoinOuter(inputs, joinType, right, leftExprIds, rightExprIds)}"
      case Multiply(left, right, _) =>
        s"${genNullCheckJoinOuter(inputs, joinType, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoinOuter(inputs, joinType, right, leftExprIds, rightExprIds)}"
      case Add(left, right, _) =>
        s"${genNullCheckJoinOuter(inputs, joinType, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoinOuter(inputs, joinType, right, leftExprIds, rightExprIds)}"
      case Divide(left, right, _) =>
        s"${genNullCheckJoinOuter(inputs, joinType, left, leftExprIds, rightExprIds)} " +
          s"&& ${genNullCheckJoinOuter(inputs, joinType, right, leftExprIds, rightExprIds)}"
      case Abs(v) =>
        s"${genNullCheckJoinOuter(inputs, joinType, v, leftExprIds, rightExprIds)}"
      case Literal(v, DoubleType | IntegerType) =>
        "true"
    }
  }

  def evaluateSub(inputs: Seq[Attribute], expression: Expression): String = {
    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx =>
            s"input_${idx}->data[i]"
        }
      case Subtract(left, right, _) =>
        s"${evaluateSub(inputs, left)} - ${evaluateSub(inputs, right)}"
      case DateSub(startDate, days) =>
        s"${evaluateSub(inputs, startDate)} - ${evaluateSub(inputs, days)}"
      case DateAdd(startDate, days) =>
        s"${evaluateSub(inputs, startDate)} + ${evaluateSub(inputs, days)}"
      case Multiply(left, right, _) =>
        s"${evaluateSub(inputs, left)} * ${evaluateSub(inputs, right)}"
      case Add(left, right, _) =>
        s"${evaluateSub(inputs, left)} + ${evaluateSub(inputs, right)}"
      case Divide(left, right, _) =>
        s"${evaluateSub(inputs, left)} / ${evaluateSub(inputs, right)}"
      case Abs(v) =>
        s"abs(${evaluateSub(inputs, v)})"
      case Literal(v, DoubleType | IntegerType | DateType) =>
        s"$v"
      case And(left, right) =>
        s"${evaluateSub(inputs, left)} && ${evaluateSub(inputs, right)}"
      case IsNotNull(_) =>
        s"1"
      case LessThan(left, right) =>
        s"${evaluateSub(inputs, left)} < ${evaluateSub(inputs, right)}"
      case GreaterThan(left, right) =>
        s"${evaluateSub(inputs, left)} > ${evaluateSub(inputs, right)}"
      case LessThanOrEqual(left, right) =>
        s"${evaluateSub(inputs, left)} < ${evaluateSub(inputs, right)}"
    }
  }

  def evaluateSubSorted(inputs: Seq[Attribute], expression: Expression): String = {
    expression match {
      case AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.name == name) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx =>
            s"input_${idx}->data[std::get<1>(sort_column_validity_buffer[i])]"
        }
      case Subtract(left, right, _) =>
        s"${evaluateSub(inputs, left)} - ${evaluateSub(inputs, right)}"
      case Multiply(left, right, _) =>
        s"${evaluateSub(inputs, left)} * ${evaluateSub(inputs, right)}"
      case Add(left, right, _) =>
        s"${evaluateSub(inputs, left)} + ${evaluateSub(inputs, right)}"
      case Divide(left, right, _) =>
        s"${evaluateSub(inputs, left)} / ${evaluateSub(inputs, right)}"
      case Abs(v) =>
        s"abs(${evaluateSub(inputs, v)})"
      case Literal(v, DoubleType | IntegerType) =>
        s"$v"
    }
  }

  def evaluateSubJoin(
    inputs: Seq[Attribute],
    expression: Expression,
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId]
  ): String = {

    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx if (leftExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[left_out[i]]"
          case idx if (rightExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[right_out[i]]"
        }
      case Subtract(left, right, _) =>
        s"${evaluateSubJoin(inputs, left, leftExprIds, rightExprIds)} - ${evaluateSubJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Multiply(left, right, _) =>
        s"${evaluateSubJoin(inputs, left, leftExprIds, rightExprIds)} * ${evaluateSubJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Add(left, right, _) =>
        s"${evaluateSubJoin(inputs, left, leftExprIds, rightExprIds)} + ${evaluateSubJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Divide(left, right, _) =>
        s"${evaluateSubJoin(inputs, left, leftExprIds, rightExprIds)} / ${evaluateSubJoin(inputs, right, leftExprIds, rightExprIds)}"
      case Abs(v) =>
        s"abs(${evaluateSubJoin(inputs, v, leftExprIds, rightExprIds)})"
      case Literal(v, DoubleType | IntegerType) =>
        s"$v"
    }
  }

  def evaluateSubJoinOuter(
    inputs: Seq[Attribute],
    expression: Expression,
    leftExprIds: Set[ExprId],
    rightExprIds: Set[ExprId]
  ): String = {

    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx if (leftExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[outer_idx[idx]]"
          case idx if (rightExprIds.contains(attr.exprId)) =>
            s"input_${idx}->data[outer_idx[idx]]"
        }
      case Subtract(left, right, _) =>
        s"${evaluateSubJoinOuter(inputs, left, leftExprIds, rightExprIds)} - ${evaluateSubJoinOuter(inputs, right, leftExprIds, rightExprIds)}"
      case Multiply(left, right, _) =>
        s"${evaluateSubJoinOuter(inputs, left, leftExprIds, rightExprIds)} * ${evaluateSubJoinOuter(inputs, right, leftExprIds, rightExprIds)}"
      case Add(left, right, _) =>
        s"${evaluateSubJoinOuter(inputs, left, leftExprIds, rightExprIds)} + ${evaluateSubJoinOuter(inputs, right, leftExprIds, rightExprIds)}"
      case Divide(left, right, _) =>
        s"${evaluateSubJoinOuter(inputs, left, leftExprIds, rightExprIds)} / ${evaluateSubJoinOuter(inputs, right, leftExprIds, rightExprIds)}"
      case Abs(v) =>
        s"abs(${evaluateSubJoinOuter(inputs, v, leftExprIds, rightExprIds)})"
      case Literal(v, DoubleType | IntegerType) =>
        s"$v"
    }
  }

  def process(
    inputs: Seq[Attribute],
    cleanName: String,
    aggregateExpression: AggregateExpression,
    idx: Int
  ): Option[AggregateDescription] = {
    PartialFunction.condOpt(aggregateExpression.aggregateFunction) {
      case Sum(sub) =>
        AggregateDescription(
          init = List(
            s"output_${idx}_sum->data = (${cTypeOfSub(inputs, sub)} *)malloc(1 * sizeof(${cTypeOfSub(inputs, sub)}));",
            s"output_${idx}_sum->count = 1;",
            s"unsigned char* output_${idx}_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
            s"output_${idx}_validity_buffer[0] = 0;",
            s"double ${cleanName}_accumulated = 0;"
          ),
          iter = List(
            s"if(${genNullCheck(inputs, sub)})",
            "{",
            s"output_${idx}_validity_buffer[0] = 1;",
            s"${cleanName}_accumulated += ${evaluateSub(inputs, sub)};",
            "}"
          ),
          result = List(
            s"output_${idx}_sum->data[0] = ${cleanName}_accumulated;",
            s"output_${idx}_sum->validityBuffer = output_${idx}_validity_buffer;"
          ),
          outputArguments = dataTypeOfSub(inputs, sub) match {
            case DoubleType  => List(s"nullable_double_vector* output_${idx}_sum")
            case IntegerType => List(s"nullable_int_vector* output_${idx}_sum")
            case LongType    => List(s"nullable_bigint_vector* output_${idx}_sum")
          }
        )
      case Average(sub) =>
        val outputSum = s"output_${idx}_average_sum"
        val outputCount = s"output_${idx}_average_count"
        AggregateDescription(
          init = List(
            s"${outputSum}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputSum}->count = 1;",
            s"${outputCount}->data = (long *)malloc(1 * sizeof(long));",
            s"${outputCount}->count = 1;",
            s"unsigned char* output_${idx}_sum_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
            s"unsigned char* output_${idx}_count_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
            s"output_${idx}_sum_validity_buffer[0] = 0;",
            s"output_${idx}_count_validity_buffer[0] = 0;",
            s"double ${cleanName}_accumulated = 0;",
            s"long ${cleanName}_counted = 0;"
          ),
          iter = List(
            s"if(${genNullCheck(inputs, sub)})",
            "{",
            s"output_${idx}_count_validity_buffer[0] = 1;",
            s"output_${idx}_sum_validity_buffer[0] = 1;",
            s"${cleanName}_accumulated += ${evaluateSub(inputs, sub)};",
            s"${cleanName}_counted += 1;",
            "}"
          ),
          result = List(
            s"${outputSum}->data[0] = ${cleanName}_accumulated;",
            s"${outputCount}->data[0] = ${cleanName}_counted;",
            s"${outputCount}->validityBuffer = output_${idx}_count_validity_buffer;",
            s"${outputSum}->validityBuffer = output_${idx}_sum_validity_buffer;"
          ),
          outputArguments =
            List(s"nullable_double_vector* ${outputSum}", s"nullable_bigint_vector* ${outputCount}")
        )
      case Count(el) =>
        val outputCount = s"output_${idx}_count"

        AggregateDescription(
          init = List(
            s"${outputCount}->data = (long *)malloc(1 * sizeof(long));",
            s"${outputCount}->count = 1;",
            s"long ${cleanName}_counted = 0;",
            s"${outputCount}->validityBuffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
            s"${outputCount}->validityBuffer[0] = 1;"
          ),
          iter = List(s"${cleanName}_counted += 1;"),
          result = List(s"${outputCount}->data[0] = ${cleanName}_counted;"),
          outputArguments = List(s"nullable_bigint_vector* ${outputCount}")
        )

      case Min(sub) =>
        val outputMin = s"output_${idx}_min"
        AggregateDescription(
          init = List(
            s"${outputMin}->data = (${cTypeOfSub(inputs, sub)} *)malloc(1 * sizeof(${cTypeOfSub(inputs, sub)}));",
            s"${outputMin}->count = 1;",
            s"${outputMin}->validityBuffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
            s"${outputMin}->validityBuffer[0] = 0;",
            s"${cTypeOfSub(inputs, sub)} ${cleanName}_min = std::numeric_limits<${cTypeOfSub(inputs, sub)}>::max();"
          ),
          iter = List(
            s"if (${cleanName}_min > ${evaluateSub(inputs, sub)} && ${genNullCheck(inputs, sub)}) {",
            s"${cleanName}_min = ${evaluateSub(inputs, sub)};",
            s"${outputMin}->validityBuffer[0] = 1;",
            "}"
          ),
          result = List(s"${outputMin}->data[0] = ${cleanName}_min;"),
          outputArguments = inputs(idx).dataType match {
            case DoubleType  => List(s"nullable_double_vector* ${outputMin}")
            case IntegerType => List(s"nullable_int_vector* ${outputMin}")
            case LongType    => List(s"nullable_bigint_vector* ${outputMin}")
          }
        )

      case Max(sub) =>
        val outputMax = s"output_${idx}_max"
        AggregateDescription(
          init = List(
            s"${outputMax}->data = (${cTypeOfSub(inputs, sub)} *)malloc(1 * sizeof(${cTypeOfSub(inputs, sub)}));",
            s"${outputMax}->count = 1;",
            s"${outputMax}->validityBuffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
            s"${outputMax}->validityBuffer[0] = 0;",
            s"${cTypeOfSub(inputs, sub)} ${cleanName}_max = std::numeric_limits<${cTypeOfSub(inputs, sub)}>::min();"
          ),
          iter = List(
            s"if (${cleanName}_max < ${evaluateSub(inputs, sub)} && ${genNullCheck(inputs, sub)}) {",
            s"${cleanName}_max = ${evaluateSub(inputs, sub)};",
            s"${outputMax}->validityBuffer[0] = 1;",
            "}"
          ),
          result = List(s"${outputMax}->data[0] = ${cleanName}_max;"),
          outputArguments = inputs(idx).dataType match {
            case DoubleType  => List(s"nullable_double_vector* ${outputMax}")
            case IntegerType => List(s"nullable_int_vector* ${outputMax}")
            case LongType    => List(s"nullable_bigint_vector* ${outputMax}")
          }
        )

      case Corr(left, right, _) =>
        val outputCorr = s"output_${idx}_corr"

        AggregateDescription(
          init = List(
            s"${outputCorr}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputCorr}->validityBuffer = (unsigned char *)malloc(1 * sizeof(unsigned char));",
            s"${outputCorr}->validityBuffer[0] = 0;",
            s"${outputCorr}->count = 1;",
            s"int non_null_count = 0;",
            s"double ${cleanName}_x_sum = 0;",
            s"double ${cleanName}_y_sum = 0;",
            s"double ${cleanName}_xy_sum = 0;",
            s"double ${cleanName}_x_square_sum = 0;",
            s"double ${cleanName}_y_square_sum = 0;"
          ),
          iter = List(
            s"if(${genNullCheck(inputs, left)} && ${genNullCheck(inputs, right)}) {",
            s"${outputCorr}->validityBuffer[0] =1;",
            "non_null_count += 1;",
            s"${cleanName}_x_sum += ${evaluateSub(inputs, left)};",
            s"${cleanName}_y_sum += ${evaluateSub(inputs, right)};",
            s"${cleanName}_xy_sum += ${evaluateSub(inputs, left)} * ${evaluateSub(inputs, right)};",
            s"${cleanName}_x_square_sum += ${evaluateSub(inputs, left)} * ${evaluateSub(inputs, left)};",
            s"${cleanName}_y_square_sum += ${evaluateSub(inputs, right)} * ${evaluateSub(inputs, right)};",
            "}"
          ),
          result = List(
            s"${outputCorr}->data[0] = (non_null_count * ${cleanName}_xy_sum - ${cleanName}_x_sum * ${cleanName}_y_sum) / " +
              s"sqrt(" +
              s"(non_null_count * ${cleanName}_x_square_sum - ${cleanName}_x_sum * ${cleanName}_x_sum) * " +
              s"(non_null_count * ${cleanName}_y_square_sum - ${cleanName}_y_sum * ${cleanName}_y_sum));"
          ),
          outputArguments = List(s"nullable_double_vector* ${outputCorr}")
        )

    }
  }

  final case class CodeLines(lines: List[String]) {
    def indented: CodeLines = CodeLines(lines = lines.map(line => s"  $line"))

    override def toString: String = (List(s"CodeLines(") ++ lines ++ List(")")).mkString("\n")

    def cCode: String = lines.mkString("\n", "\n", "\n")

    def append(codeLines: CodeLines*): CodeLines = copy(lines = lines ++ codeLines.flatMap(_.lines))
  }

  object CodeLines {
    def from(str: CodeLines*): CodeLines = CodeLines(lines = str.flatMap(_.lines).toList)

    implicit def stringToCodeLines(str: String): CodeLines = CodeLines(List(str))

    implicit def listStringToCodeLines(str: List[String]): CodeLines = CodeLines(str)

    implicit def listCodeLines(str: List[CodeLines]): CodeLines = CodeLines(str.flatMap(_.lines))

    def empty: CodeLines = CodeLines(Nil)
  }

  implicit class RichListStr(list: List[String]) {
    def codeLines: CodeLines = CodeLines(list)
  }

  trait NameCleaner {
    def cleanName(input: String): String
  }

  object NameCleaner {
    val simple: NameCleaner = _.replaceAll("[^A-Z_a-z0-9]", "")
    val verbose: NameCleaner = v => CleanName.fromString(v).value
  }

  def filterInputs(cond: Expression, input: Seq[Attribute]): List[String] = CFunctionGeneration
    .generateFilter(
      CFunctionGeneration.VeFilter(
        input.zipWithIndex.map { case (attribute, i) =>
          CFunctionGeneration.CVector(s"input_$i", veType(attribute.dataType))
        }.toList,
        CFunctionGeneration.CExpression(evaluateSub(input, cond), None)
      )
    )
    .lines

  def cGen(
    fName: String,
    inputReferences: Set[String],
    childOutputs: Seq[Attribute],
    pairs: Seq[(Alias, AggregateExpression)],
    condition: Option[Expression] = None
  )(implicit nameCleaner: NameCleaner): CodeLines = {
    val input = {
      val attrs = childOutputs
        .filter(attr => inputReferences.contains(attr.name))

      if (childOutputs.size > attrs.size) childOutputs else attrs
    }

    val cleanNames = pairs.map(_._1.name).map(nameCleaner.cleanName).toList
    val ads = cleanNames.zip(pairs).zipWithIndex.map {
      case ((cleanName, (alias, aggregateExpression)), idx) =>
        process(input, cleanName, aggregateExpression, idx)
          .getOrElse(sys.error(s"Unknown: $aggregateExpression"))
    }

    val inputBits = input.zipWithIndex
      .map { case (i, idx) =>
        s"nullable_double_vector* input_${idx}"
      }
      .mkString(", ")

    List[List[String]](
      List(s"""extern "C" long ${fName}(${inputBits}, ${ads
        .flatMap(_.outputArguments)
        .mkString(", ")}) {"""),
      condition.toList.flatMap(cond => filterInputs(cond, input)),
      ads.flatMap(_.init),
      List("#pragma _NEC ivdep"),
      List("for (int i = 0; i < input_0->count; i++) {"),
      ads.flatMap(_.iter),
      List("}"),
      ads.flatMap(_.result),
      List("return 0;")
    ).flatten.codeLines
  }




}
