package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  BinaryOperator,
  CaseWhen,
  Cast,
  Coalesce,
  Contains,
  EndsWith,
  EqualTo,
  ExprId,
  Expression,
  Greatest,
  If,
  IsNotNull,
  IsNull,
  KnownFloatingPointNormalized,
  Least,
  Literal,
  Not,
  Sqrt,
  StartsWith
}
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

  def evalString(
    expression: Expression
  )(implicit fallback: EvalFallback): Either[Expression, StringProducer] =
    expression match {
      case CaseWhen(
            Seq((predicate, Literal(t: UTF8String, StringType))),
            Some(Literal(f: UTF8String, StringType))
          ) =>
        eval(predicate).map { ce =>
          StringProducer.StringChooser(ce, t.toString, f.toString)
        }
      case AttributeReference(name, StringType, _, _) =>
        Right(StringProducer.copyString(name))
    }

  /** Enable a fallback in the evaluation, so that we can inject custom mappings where matches are not found. */
  trait EvalFallback { ef =>
    def fallback: PartialFunction[Expression, CExpression]

    final def unapply(input: Expression): Option[CExpression] = fallback.lift(input)

    final def orElse(elseCase: EvalFallback): EvalFallback = new EvalFallback {
      override def fallback: PartialFunction[Expression, CExpression] =
        ef.fallback.orElse(elseCase.fallback)
    }
  }

  object EvalFallback {
    def noOp: EvalFallback = NoOpFallback

    private object NoOpFallback extends EvalFallback {
      override def fallback: PartialFunction[Expression, CExpression] = PartialFunction.empty
    }

    def from(pf: PartialFunction[Expression, CExpression]): EvalFallback = new EvalFallback {
      override def fallback: PartialFunction[Expression, CExpression] = pf
    }
  }

  /** Either we return a successful expression, or point to the errored expression */
  type EvaluationAttempt = Either[Expression, CExpression]

  object EvaluationAttempt {
    implicit class RichEvaluationAttempt(evaluationAttempt: EvaluationAttempt) {
      def getOrReport(): CExpression = {
        evaluationAttempt.fold(
          expr => sys.error(s"Could not handle the expression ${expr}, type ${expr.getClass}"),
          identity
        )
      }
    }
    implicit class RichEvaluationAttemptStr(evaluationAttempt: Either[Expression, StringProducer]) {
      def getOrReport(): StringProducer = {
        evaluationAttempt.fold(
          expr => sys.error(s"Could not handle the expression ${expr}, type ${expr.getClass}"),
          identity
        )
      }
    }
  }

  def eval(expression: Expression)(implicit fallback: EvalFallback): EvaluationAttempt = {
    expression match {
      case EqualTo(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        Right {
          CExpression(
            cCode = List(
              s"std::string(${left.name}->data, ${left.name}->offsets[i], ${left.name}->offsets[i+1]-${left.name}->offsets[i])",
              s"""std::string("${right.toString()}")"""
            ).mkString(" == "),
            isNotNullCode = None
          )
        }
      case Contains(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        Right {
          CExpression(
            cCode = {
              val mainString =
                s"std::string(${left.name}->data, ${left.name}->offsets[i], ${left.name}->offsets[i+1]-${left.name}->offsets[i])"
              val rightString = s"""std::string("${right.toString()}")"""
              s"${mainString}.find(${rightString}) != std::string::npos"
            },
            isNotNullCode = None
          )
        }
      case EndsWith(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        Right {
          CExpression(
            cCode = {
              val leftStringLength =
                s"(${left.name}->offsets[i+1] - ${left.name}->offsets[i])"
              val expectedLength = right.toString().length
              val leftStringSubstring =
                s"""std::string(${left.name}->data, ${left.name}->offsets[i+1]-${expectedLength}, ${expectedLength})"""
              val rightString = s"""std::string("${right.toString()}")"""
              s"${leftStringLength} >= ${expectedLength} && ${leftStringSubstring} == ${rightString}"
            },
            isNotNullCode = None
          )
        }
      case StartsWith(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        Right {
          CExpression(
            cCode = {
              val leftStringLength =
                s"(${left.name}->offsets[i+1] - ${left.name}->offsets[i])"
              val expectedLength = right.toString().length
              val leftStringSubstring =
                s"""std::string(${left.name}->data, ${left.name}->offsets[i], ${expectedLength})"""
              val rightString = s"""std::string("${right.toString()}")"""
              s"${leftStringLength} >= ${expectedLength} && ${leftStringSubstring} == ${rightString}"
            },
            isNotNullCode = None
          )
        }
      case b: BinaryOperator =>
        for {
          leftEx <- eval(b.left)
          rightEx <- eval(b.right)
        } yield CExpression(
          cCode = s"((${leftEx.cCode}) ${binaryOperatorOverride
            .getOrElse(b.symbol, b.symbol)} (${rightEx.cCode}))",
          isNotNullCode = Option(
            (leftEx.isNotNullCode.toList ++
              rightEx.isNotNullCode.toList)
          ).filter(_.nonEmpty).map(_.mkString("(", " && ", ")"))
        )
      case KnownFloatingPointNormalized(child) => eval(child)
      case NormalizeNaNAndZero(child)          => eval(child)
      case Sqrt(c) =>
        eval(c).map { ex =>
          CExpression(cCode = s"sqrt(${ex.cCode})", isNotNullCode = ex.isNotNullCode)
        }
      case Coalesce(children) if children.size == 1 =>
        eval(children.head)
      case Coalesce(children) =>
        eval(children.head).flatMap { first =>
          first.isNotNullCode match {
            case None => Right(first)
            case Some(notNullCheck) =>
              eval(Coalesce(children.drop(1))).map { sub =>
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
          }
        }
      case ar: AttributeReference if ar.name.endsWith("_nullable") =>
        Right(CExpression(cCode = ar.name, isNotNullCode = Some(s"${ar.name}_is_set")))
      case ar: AttributeReference if ar.name.contains("data[") =>
        Right {
          CExpression(
            cCode = ar.name,
            isNotNullCode = {
              val indexingExpr = ar.name.substring(0, ar.name.length - 1).split("""data(\[)""")
              Some(
                s"check_valid(${ar.name.replaceAll("""data\[.*\]""", "validityBuffer")}, ${indexingExpr(indexingExpr.size - 1)})"
              )
            }
          )
        }
      case IsNull(child) =>
        eval(child).map { ex =>
          CExpression(
            cCode = ex.isNotNullCode match {
              case None => "0"
              case Some(notNullCode) =>
                s"!(${notNullCode})"
            },
            // result is never null here
            isNotNullCode = None
          )
        }
      case IsNotNull(child) =>
        eval(child).map { ex =>
          CExpression(
            cCode = ex.isNotNullCode match {
              case None => "1"
              case Some(notNullCode) =>
                notNullCode
            },
            // result is never null here
            isNotNullCode = None
          )
        }
      case If(predicate, trueValue, falseValue) =>
        for {
          p <- eval(predicate)
          t <- eval(trueValue)
          f <- eval(falseValue)
        } yield CExpression(
          cCode = s"(${p.cCode}) ? (${t.cCode}) : (${f.cCode})",
          isNotNullCode = None
        )
      case Literal(null, d) =>
        Right {
          CExpression(cCode = s"0", isNotNullCode = Some("0"))
        }
      case Literal(v, d) =>
        Right {
          CExpression(cCode = s"$v", isNotNullCode = None)
        }
      case Cast(child, newDt, None) =>
        eval(child).map { ex =>
          CExpression(
            cCode = s"(${sparkTypeToScalarVeType(newDt).cScalarType}) (${ex.cCode})",
            isNotNullCode = ex.isNotNullCode
          )
        }
      case Not(child) =>
        eval(child).map { ex =>
          CExpression(cCode = s"!(${ex.cCode})", isNotNullCode = ex.isNotNullCode)
        }
      case Greatest(children) =>
        val fails = children.map(exp => eval(exp)).flatMap(_.left.toOption)
        fails.headOption.toLeft {
          val oks = children.map(exp => eval(exp)).flatMap(_.right.toOption)
          FlatToNestedFunction.runWhenNotNull(items = oks.toList, function = "std::max")
        }
      case Least(children) =>
        val fails = children.map(exp => eval(exp)).flatMap(_.left.toOption)
        fails.headOption.toLeft {
          val oks = children.map(exp => eval(exp)).flatMap(_.right.toOption)
          FlatToNestedFunction.runWhenNotNull(items = oks.toList, function = "std::min")
        }
      case Cast(child, dataType, _) =>
        dataType match {
          case IntegerType =>
            eval(child).map { childExpression =>
              childExpression.copy("((int)" + childExpression.cCode + ")")
            }

          case DoubleType =>
            eval(child).map { childExpression =>
              childExpression.copy("((int)" + childExpression.cCode + ")")
            }

          case LongType =>
            eval(child).map { childExpression =>
              childExpression.copy("((long long)" + childExpression.cCode + ")")
            }
        }
      case NoOp =>
        Right(CExpression("0", Some("false")))
      case CaseWhen(Seq((caseExp, valueExp)), None) =>
        for {
          cond <- eval(caseExp)
          value <- eval(valueExp)
        } yield CExpression(
          cCode = value.cCode,
          isNotNullCode = {
            (cond.isNotNullCode, value.isNotNullCode) match {
              case (None, None) =>
                Some(cond.cCode)
              case (Some(condNotNull), None) =>
                Some(s"(${condNotNull}) && (${cond.cCode})")
              case (None, Some(valueNotNull)) =>
                Some(s"(${cond.cCode}) && ${valueNotNull})")
              case (Some(condNotNull), Some(valueNotNull)) =>
                Some(s"($condNotNull) && (${cond.cCode}) && (${valueNotNull})")
            }
          }
        )
      case CaseWhen(Seq((caseExp, valueExp), xs @ _*), None) =>
        eval(CaseWhen(Seq((caseExp, valueExp)), Some(CaseWhen(xs))))
      case CaseWhen(Seq((caseExp, valueExp)), Some(elseValue)) =>
        for {
          cond <- eval(caseExp)
          value <- eval(valueExp)
          ev <- eval(elseValue)
        } yield {
          val willBeTrue = cond.isNotNullCode match {
            case None                => cond.cCode
            case Some(condIsNotNull) => s"(${condIsNotNull}) && (${cond.cCode})"
          }
          CExpression(
            cCode = s"(${willBeTrue}) ? (${value.cCode}) : (${ev.cCode})",
            isNotNullCode = {
              (value.isNotNullCode, ev.isNotNullCode) match {
                case (None, None) => None
                case (Some(valueNotNull), None) =>
                  Some(s"(${willBeTrue}) ? (${valueNotNull}) : 1")
                case (None, Some(evNotNull)) =>
                  Some(s"(${willBeTrue}) ? 1 : (${evNotNull})")
                case (Some(valueNotNull), Some(evNotNull)) =>
                  Some(s"(${willBeTrue}) ? (${valueNotNull}) : (${evNotNull})")
              }
            }
          )
        }
      case CaseWhen(Seq((caseExp, valueExp), xs @ _*), Some(elseValue)) =>
        eval(CaseWhen(Seq((caseExp, valueExp)), CaseWhen(xs, elseValue)))
      case fallback(result) =>
        Right(result)
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
  def sparkTypeToVeType(dataType: DataType): VeType = {
    dataType match {
      case DoubleType  => VeScalarType.veNullableDouble
      case IntegerType => VeScalarType.veNullableInt
      case LongType    => VeScalarType.veNullableLong
      case ShortType   => VeScalarType.veNullableInt
      case BooleanType => VeScalarType.veNullableInt
      case StringType  => VeString
    }
  }
}
