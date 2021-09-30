package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration._

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, NoOp, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BinaryOperator, CaseWhen, Cast, Coalesce, EqualTo, ExprId, Expression, Greatest, If, IsNotNull, IsNull, KnownFloatingPointNormalized, Least, Literal, Sqrt}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object SparkVeMapper {

  def referenceReplacer(inputs: Seq[Attribute]): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference =>
      inputs.indexWhere(_.exprId == ar.exprId) match {
        case -1 =>
          sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
        case idx =>
          if (ar.dataType == StringType)
            ar.withName(s"input_${idx}")
          else
            ar.withName(s"input_${idx}->data[i]")
      }
  }

  def referenceOutputReplacer(inputs: Seq[Attribute]): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference =>
      inputs.indexWhere(_.exprId == ar.exprId) match {
        case -1 =>
          sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
        case idx =>
          if (ar.dataType == StringType)
            ar.withName(s"output_${idx}")
          else
            ar.withName(s"output_${idx}->data[i]")
      }
  }

  def referenceReplacer(
    inputs: Seq[Attribute],
    leftIds: Set[ExprId],
    rightIds: Set[ExprId]
  ): PartialFunction[Expression, Expression] = { case ar: AttributeReference =>
    inputs.indexWhere(_.exprId == ar.exprId) match {
      case -1 =>
        sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
      case idx if (leftIds.contains(ar.exprId)) =>
        ar.withName(s"input_${idx}->data[left_out[i]]")
      case idx if (rightIds.contains(ar.exprId)) =>
        ar.withName(s"input_${idx}->data[right_out[i]]")
    }
  }

  def referenceReplacerOuter(
    inputs: Seq[Attribute],
    leftIds: Set[ExprId],
    rightIds: Set[ExprId],
    joinType: JoinType
  ): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference => {
      val outerJoinIds = joinType match {
        case RightOuterJoin => rightIds
        case LeftOuterJoin  => leftIds
      }

      inputs.indexWhere(_.exprId == ar.exprId) match {
        case -1 =>
          sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
        case idx if (outerJoinIds.contains(ar.exprId)) =>
          ar.withName(s"input_${idx}->data[outer_idx[idx]]")
        case _ => NoOp
      }
    }
  }

  def replaceReferences(inputs: Seq[Attribute], expression: Expression): Expression =
    expression.transform(referenceReplacer(inputs))

  def replaceOutputReferences(inputs: Seq[Attribute], expression: Expression): Expression =
    expression.transform(referenceOutputReplacer(inputs))

  def replaceReferences(
    inputs: Seq[Attribute],
    expression: Expression,
    leftIds: Set[ExprId],
    rightIds: Set[ExprId]
  ): Expression =
    expression.transform(referenceReplacer(inputs, leftIds, rightIds))

  def replaceReferencesOuter(
    inputs: Seq[Attribute],
    expression: Expression,
    leftIds: Set[ExprId],
    rightIds: Set[ExprId],
    joinType: JoinType
  ): Expression =
    expression.transform(referenceReplacerOuter(inputs, leftIds, rightIds, joinType))

  val binaryOperatorOverride = Map("=" -> "==")

  def evalString(expression: Expression, fallback: EvalFallback): StringProducer =
    expression match {
      case CaseWhen(
            Seq((predicate, Literal(t: UTF8String, StringType))),
            Some(Literal(f: UTF8String, StringType))
          ) =>
        StringProducer.StringChooser(eval(predicate, fallback), t.toString, f.toString)
      case AttributeReference(name, StringType, _, _) =>
        StringProducer.copyString(name)
      case other =>
        sys.error(
          s"Expression of type [${expression.getClass}] not yet supported for String evaluation (${expression})"
        )
    }

  /** Enable a fallback in the evaluation, so that we can inject custom mappings where matches are not found. */
  trait EvalFallback {
    def fallback: PartialFunction[Expression, CExpression]

    final def unapply(input: Expression): Option[CExpression] = fallback.lift(input)
  }

  object EvalFallback {
    def noOp: EvalFallback = NoOpFallback

    object NoOpFallback extends EvalFallback {
      override def fallback: PartialFunction[Expression, CExpression] = PartialFunction.empty
    }

    object AggregationProjectionFallback extends EvalFallback {

      override def fallback: PartialFunction[Expression, CExpression] = {
        case Sum(child) => eval(child, this)
        case Average(child) => eval(child, this)
        case Alias(child, _) => eval(child, this)
        case AggregateExpression(Sum(child), mode, isDistinct, filter, resultId) => eval(child, this)
        case AggregateExpression(Average(child), mode, isDistinct, filter, resultId) => eval(child, this)

      }
    }
  }

  def eval(expression: Expression, fallback: EvalFallback): CExpression = {
    expression match {
      case EqualTo(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        CExpression(
          cCode = List(
            s"std::string(${left.name}->data, ${left.name}->offsets[i], ${left.name}->offsets[i+1]-${left.name}->offsets[i])",
            s"""std::string("${right.toString()}")"""
          ).mkString(" == "),
          isNotNullCode = None
        )
      case b: BinaryOperator =>
        CExpression(
          cCode = s"((${eval(b.left, fallback).cCode}) ${binaryOperatorOverride
            .getOrElse(b.symbol, b.symbol)} (${eval(b.right, fallback).cCode}))",
          isNotNullCode = Option(
            (eval(b.left, fallback).isNotNullCode.toList ++
              eval(b.right, fallback).isNotNullCode.toList)
          ).filter(_.nonEmpty).map(_.mkString("(", " && ", ")"))
        )
      case KnownFloatingPointNormalized(child) => eval(child, fallback)
      case NormalizeNaNAndZero(child)          => eval(child, fallback)
      case Sqrt(c) =>
        CExpression(
          cCode = s"sqrt(${eval(c, fallback).cCode})",
          isNotNullCode = eval(c, fallback).isNotNullCode
        )
      case Coalesce(children) if children.size == 1 =>
        eval(children.head, fallback)
      case Coalesce(children) =>
        val first = eval(children.head, fallback)

        first.isNotNullCode match {
          case None => first
          case Some(notNullCheck) =>
            val sub = eval(Coalesce(children.drop(1)), fallback)
            CExpression(
              cCode = s"(${notNullCheck}) ? ${first.cCode} : ${sub.cCode}",
              isNotNullCode = sub.isNotNullCode match {
                case None =>
                  None
                case Some(subNotNullCheck) =>
                  Some(s"((${notNullCheck}) || (${subNotNullCheck}))")
              }
            )
        }
      case ar: AttributeReference =>
        if (ar.name.endsWith("_nullable"))
          CExpression(cCode = ar.name, isNotNullCode = Some(s"${ar.name}_is_set"))
        else
          CExpression(
            cCode = ar.name,
            isNotNullCode = if (ar.name.contains("data[")) {
              val indexingExpr = ar.name.substring(0, ar.name.length - 1).split("""data(\[)""")
              Some(
                s"check_valid(${ar.name.replaceAll("""data\[.*\]""", "validityBuffer")}, ${indexingExpr(indexingExpr.size - 1)})"
              )
            } else None
          )
      case IsNull(child) =>
        CExpression(
          cCode = eval(child, fallback).isNotNullCode match {
            case None => "0"
            case Some(notNullCode) =>
              s"!(${notNullCode})"
          },
          // result is never null here
          isNotNullCode = None
        )
      case IsNotNull(child) =>
        CExpression(
          cCode = eval(child, fallback).isNotNullCode match {
            case None => "1"
            case Some(notNullCode) =>
              notNullCode
          },
          // result is never null here
          isNotNullCode = None
        )
      case If(predicate, trueValue, falseValue) =>
        CExpression(
          cCode =
            s"(${eval(predicate, fallback).cCode}) ? (${eval(trueValue, fallback).cCode}) : (${eval(falseValue, fallback).cCode})",
          isNotNullCode = None
        )
      case Literal(null, d) =>
        CExpression(cCode = s"0", isNotNullCode = Some("0"))
      case Literal(v, d) =>
        CExpression(cCode = s"$v", isNotNullCode = None)
      case Cast(child, newDt, None) =>
        CExpression(
          cCode = s"(${sparkTypeToScalarVeType(newDt).cScalarType}) ${eval(child, fallback).cCode}",
          isNotNullCode = eval(child, fallback).isNotNullCode
        )
      case Greatest(children) =>
        FlatToNestedFunction.runWhenNotNull(
          items = children.map(exp => eval(exp, fallback)).toList,
          function = "std::max"
        )
      case Least(children) =>
        FlatToNestedFunction.runWhenNotNull(
          items = children.map(exp => eval(exp, fallback)).toList,
          function = "std::min"
        )
      case Cast(child, dataType, _) =>
        dataType match {
          case IntegerType => {
            val childExpression = eval(child, fallback)
            childExpression.copy("((int)" + childExpression.cCode + ")")
          }

          case DoubleType => {
            val childExpression = eval(child, fallback)
            childExpression.copy("((int)" + childExpression.cCode + ")")
          }

          case LongType => {
            val childExpression = eval(child, fallback)
            childExpression.copy("((long long)" + childExpression.cCode + ")")
          }
        }
      case NoOp =>
        CExpression("0", Some("false"))
      case CaseWhen(branches, Some(elseValue)) =>
        branches match {
          case Seq((caseExp, valueExp)) =>
            val cond = eval(caseExp, fallback)
            val value = eval(valueExp, fallback)

            CExpression(
              cCode = s"(${cond.cCode}) ? (${value.cCode}) : (${eval(elseValue, fallback).cCode})",
              isNotNullCode = Some(s"(${cond.cCode}) ? (${value.cCode}) : (${eval(elseValue, fallback).cCode})")
            )
        }
      case fallback(result) =>
        result
      case other =>
        sys.error(
          s"not supported ${other}: " + expression.getClass.getCanonicalName + ": " + expression
            .toString()
        )
    }
  }

  def sparkTypeToScalarVeType(dataType: DataType): VeScalarType = {
    dataType match {
      case DoubleType  => VeScalarType.veNullableDouble
      case IntegerType => VeScalarType.veNullableInt
      case LongType    => VeScalarType.veNullableLong
      case ShortType   => VeScalarType.veNullableInt
      case BooleanType => VeScalarType.veNullableInt
    }
  }
}
