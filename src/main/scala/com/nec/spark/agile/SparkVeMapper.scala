package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration.{CExpression, JoinType, LeftOuterJoin, RightOuterJoin, VeType}

import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryOperator, Cast, Coalesce, Expression, Greatest, If, IsNotNull, IsNull, Least, Literal, Sqrt}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryArithmetic, Cast, Coalesce, ExprId, Expression, Greatest, If, IsNotNull, IsNull, KnownFloatingPointNormalized, Least, Literal}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.{LeftOuter, RightOuter}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}

object SparkVeMapper {

  def referenceReplacer(inputs: Seq[Attribute]): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference =>
      inputs.indexWhere(_.exprId == ar.exprId) match {
        case -1 =>
          sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
        case idx =>
          ar.withName(s"input_${idx}->data[i]")
      }
  }

  def referenceReplacer(inputs: Seq[Attribute], leftIds: Set[ExprId],
                        rightIds: Set[ExprId]): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference =>
      inputs.indexWhere(_.exprId == ar.exprId) match {
        case -1 =>
          sys.error(s"Could not find a reference for ${ar} from set of: ${inputs}")
        case idx if(leftIds.contains(ar.exprId)) =>
          ar.withName(s"input_${idx}->data[left_out[i]]")
        case idx if(rightIds.contains(ar.exprId)) =>
          ar.withName(s"input_${idx}->data[right_out[i]]")
      }
  }

  def referenceReplacerOuter(inputs: Seq[Attribute], leftIds: Set[ExprId],
                        rightIds: Set[ExprId], joinType: JoinType): PartialFunction[Expression, Expression] = {
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

  def replaceReferences(inputs: Seq[Attribute], expression: Expression, leftIds: Set[ExprId], rightIds: Set[ExprId]): Expression =
    expression.transform(referenceReplacer(inputs, leftIds, rightIds))

  def replaceReferencesOuter(inputs: Seq[Attribute], expression: Expression, leftIds: Set[ExprId], rightIds: Set[ExprId], joinType: JoinType): Expression =
    expression.transform(referenceReplacerOuter(inputs, leftIds, rightIds, joinType))

  val binaryOperatorOverride = Map("=" -> "==")

  def eval(expression: Expression): CExpression = {
    expression match {
      case b: BinaryOperator =>
        CExpression(
          cCode = s"((${eval(b.left).cCode}) ${binaryOperatorOverride
            .getOrElse(b.symbol, b.symbol)} (${eval(b.right).cCode}))",
          isNotNullCode = Option(
            (eval(b.left).isNotNullCode.toList ++
              eval(b.right).isNotNullCode.toList)
          ).filter(_.nonEmpty).map(_.mkString("(", " && ", ")"))
        )
      case KnownFloatingPointNormalized(child) => eval(child)
      case NormalizeNaNAndZero(child) => eval(child)
      case Sqrt(c) =>
        CExpression(cCode = s"sqrt(${eval(c).cCode})", isNotNullCode = eval(c).isNotNullCode)
      case Coalesce(children) if children.size == 1 =>
        eval(children.head)
      case Coalesce(children) =>
        val first = eval(children.head)

        first.isNotNullCode match {
          case None => first
          case Some(notNullCheck) =>
            val sub = eval(Coalesce(children.drop(1)))
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
            isNotNullCode =
              if (ar.name.contains("data[")) {
                val indexingExpr = ar.name.substring(0, ar.name.length-1).split("""data(\[)""")
                Some(s"check_valid(${ar.name.replaceAll("""data\[.*\]""", "validityBuffer")}, ${indexingExpr(indexingExpr.size-1)})")
              } else None
          )
      case IsNull(child) =>
        CExpression(
          cCode = eval(child).isNotNullCode match {
            case None => "0"
            case Some(notNullCode) =>
              s"!(${notNullCode})"
          },
          // result is never null here
          isNotNullCode = None
        )
      case IsNotNull(child) =>
        CExpression(
          cCode = eval(child).isNotNullCode match {
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
            s"(${eval(predicate).cCode}) ? (${eval(trueValue).cCode}) : (${eval(falseValue).cCode})",
          isNotNullCode = None
        )
      case Literal(null, d) =>
        CExpression(cCode = s"0", isNotNullCode = Some("0"))
      case Literal(v, d) =>
        CExpression(cCode = s"$v", isNotNullCode = None)
      case Cast(child, newDt, None) =>
        CExpression(
          cCode = s"(${sparkTypeToVeType(newDt).cScalarType}) ${eval(child).cCode}",
          isNotNullCode = eval(child).isNotNullCode
        )
      case Greatest(children) =>
        FlatToNestedFunction.runWhenNotNull(
          items = children.map(exp => eval(exp)).toList,
          function = "std::max"
        )
      case Least(children) =>
        FlatToNestedFunction.runWhenNotNull(
          items = children.map(exp => eval(exp)).toList,
          function = "std::min"
        )
      case NoOp =>
        CExpression(
          "0",
          Some("false")
        )
      case _ =>
        sys.error(expression.getClass.getCanonicalName + ": " + expression.toString())
    }
  }

  def sparkTypeToVeType(dataType: DataType): VeType = {
    dataType match {
      case DoubleType  => VeType.veNullableDouble
      case IntegerType => VeType.veNullableInt
      case LongType    => VeType.veNullableLong
    }
  }
}
