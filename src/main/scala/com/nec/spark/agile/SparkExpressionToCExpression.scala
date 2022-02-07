/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  BinaryOperator,
  CaseWhen,
  Cast,
  Coalesce,
  Divide,
  ExprId,
  Expression,
  Greatest,
  If,
  IsNaN,
  IsNotNull,
  IsNull,
  KnownFloatingPointNormalized,
  Least,
  Like,
  Literal,
  Not,
  SortDirection,
  Sqrt,
  Year
}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/**
 * Utility to convert from Spark's expressions to CExpressions for scalars
 */
object SparkExpressionToCExpression {

  def referenceReplacer(
    prefix: String,
    inputs: Seq[Attribute]
  ): PartialFunction[Expression, Expression] = { case ar: AttributeReference =>
    inputs.indexWhere(_.exprId == ar.exprId) match {
      case -1 =>
        sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
      case idx if ar.dataType == StringType =>
        ar.withName(s"${prefix}${idx}")
      case idx =>
        ar.withName(s"${prefix}${idx}->data[i]")
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
      case _ => sys.error(s"SparkExpressionToCExpression.referenceReplacer: " +
        s"Could not match ${Try(inputs.indexWhere(_.exprId == ar.exprId).toString)}, " +
        s"type ${inputs.indexWhere(_.exprId == ar.exprId).getClass}")
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

  def replaceReferences(
    prefix: String,
    inputs: Seq[Attribute],
    expression: Expression
  ): Expression =
    expression.transform(referenceReplacer(prefix, inputs))

  def replaceOutputReferences(inputs: Seq[Attribute], expression: Expression): Expression =
    expression.transform(referenceOutputReplacer(inputs))

  def replaceReferencesS(
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
      case Alias(AttributeReference(name, StringType, _, _), name2) =>
        Right(StringProducer.copyString(name))
      case _ => sys.error(s"SparkExpressionToCExpression.evalString: " +
        s"Could not match ${Try(expression.toString)}, " +
        s"type ${expression.getClass}")
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
      def getOrReport(): CExpression = reportToString.fold(sys.error, identity)

      def reportToString: Either[String, CExpression] =
        evaluationAttempt.left.map(expr =>
          s"Could not handle the expression ${expr}, type ${expr.getClass}, Spark type ${expr.dataType}"
        )
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
      case StringHole(_, evl) =>
        Right(evl.fetchResult)
      case b @ Divide(_, _, false) =>
        for {
          leftEx <- eval(b.left)
          rightEx <- eval(b.right)
        } yield CExpression(
          cCode = s"((${leftEx.cCode}) ${binaryOperatorOverride
            .getOrElse(b.symbol, b.symbol)} (${rightEx.cCode}))",
          isNotNullCode = Option(
            List(
              List(s"(${rightEx.cCode} != 0)"),
              leftEx.isNotNullCode.toList,
              rightEx.isNotNullCode.toList
            ).reduce(_ ++ _)
          ).filter(_.nonEmpty).map(_.mkString("(", " && ", ")"))
        )
      case b: BinaryOperator =>
        for {
          leftEx <- eval(b.left)
          rightEx <- eval(b.right)
        } yield CExpression(
          cCode = s"((${leftEx.cCode}) ${binaryOperatorOverride
            .getOrElse(b.symbol, b.symbol)} (${rightEx.cCode}))",
          isNotNullCode = Option(
            leftEx.isNotNullCode.toList ++
              rightEx.isNotNullCode.toList
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
      case Alias(child, _) =>
        eval(child)
      case ar: AttributeReference if ar.name.endsWith("_nullable") =>
        Right(CExpression(cCode = ar.name, isNotNullCode = Some(s"${ar.name}_is_set")))
      case ar: AttributeReference if ar.name.contains("data[") =>
        Right {
          CExpression(
            cCode = ar.name,
            isNotNullCode = if (ar.nullable) {
              val indexingExpr = ar.name.substring(0, ar.name.length - 1).split("""data(\[)""")
              Some(
                s"check_valid(${ar.name.replaceAll("""data\[.*\]""", "validityBuffer")}, ${indexingExpr(indexingExpr.size - 1)})"
              )
            } else None
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
      case IsNaN(child) =>
        eval(child).map { ex =>
          CExpression(
            cCode = s"std::isnan(static_cast<double>(${ex.cCode}))",
            isNotNullCode = ex.isNotNullCode
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
      case Cast(child, IntegerType, _) =>
        eval(child).map { childExpression =>
          childExpression.copy("((int)" + childExpression.cCode + ")")
        }

      case Cast(child, DoubleType, _) =>
        eval(child).map { childExpression =>
          childExpression.copy("((int)" + childExpression.cCode + ")")
        }

      case Cast(child, LongType, _) =>
        eval(child).map { childExpression =>
          childExpression.copy("((long long)" + childExpression.cCode + ")")
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
      case Year(child) =>
        for {
          childEx <- eval(child)
        } yield {
          child.dataType match {
            case DateType =>
              /*
                Since Dates are day (integer) offsets from 1970-01-01, we
                multiply by nanoseconds per day to get the timestamp.
               */
              CExpression(
                s"frovedis::year_from_datetime(${childEx.cCode} * int64_t(86400000000000))",
                childEx.isNotNullCode
              )

            case TimestampType =>
              CExpression(s"frovedis::year_from_datetime(${childEx.cCode})", childEx.isNotNullCode)

            case other =>
              throw new IllegalArgumentException(
                s"Cannot generate year extraction code for datatype: ${other}"
              )
          }
        }
      case fallback(result) => Right(result)
      case other            => Left(other)
    }
  }

  def sparkTypeToScalarVeType(dataType: DataType): VeScalarType = {
    dataType match {
      case DoubleType    => VeScalarType.veNullableDouble
      case IntegerType   => VeScalarType.veNullableInt
      case LongType      => VeScalarType.veNullableLong
      case ShortType     => VeScalarType.veNullableInt
      case DateType      => VeScalarType.veNullableInt
      case BooleanType   => VeScalarType.veNullableInt
      case TimestampType => VeScalarType.veNullableLong
    }
  }
  def sparkTypeToVeType(dataType: DataType): VeType = {
    dataType match {
      case DoubleType    => VeScalarType.veNullableDouble
      case IntegerType   => VeScalarType.veNullableInt
      case DateType      => VeScalarType.veNullableInt
      case LongType      => VeScalarType.veNullableLong
      case ShortType     => VeScalarType.veNullableInt
      case BooleanType   => VeScalarType.veNullableInt
      case StringType    => VeString
      case TimestampType => VeScalarType.veNullableLong
    }
  }
  def sparkSortDirectionToSortOrdering(sortDirection: SortDirection): SortOrdering = {
    sortDirection match {
      case expressions.Ascending  => Ascending
      case expressions.Descending => Descending
    }
  }

  def likelySparkType(veType: VeType): DataType = {
    veType match {
      case VeScalarType.VeNullableDouble => DoubleType
      case VeScalarType.VeNullableFloat  => FloatType
      case VeScalarType.VeNullableInt    => IntegerType
      case VeScalarType.VeNullableLong   => LongType
      case VeString                      => StringType
    }
  }
}
