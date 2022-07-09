package io.sparkcyclone.spark.codegen.join

import io.sparkcyclone.spark.codegen.CFunctionGeneration
import io.sparkcyclone.spark.codegen.SparkExpressionToCExpression.sparkTypeToVeType
import io.sparkcyclone.native.code.CVector
import io.sparkcyclone.spark.codegen.join.GenericJoiner.FilteredOutput
import io.sparkcyclone.spark.transformation.VERewriteStrategy.InputPrefix
import org.apache.spark.sql.catalyst.expressions.{
  And,
  AttributeReference,
  EqualTo,
  KnownFloatingPointNormalized
}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.{logical, Inner}

final case class JoinMatcher(
  leftChild: LogicalPlan,
  rightChild: LogicalPlan,
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  genericJoiner: GenericJoiner
)
object JoinMatcher {

  /**
   * This is a Join by function eg f(a.x) = f(b.y)
   * This is currently not supported, and the underlying C++ code probably should be not refactored yet
   * to accommodate for the possibility we may have to change the shape of the code.
   */
  val JoinWithFunctionSupported = false

  def unapply(logicalPlan: LogicalPlan): Option[JoinMatcher] = {
    PartialFunction.condOpt(logicalPlan) {
      case j @ logical.Join(
            leftChild,
            rightChild,
            Inner,
            Some(
              condition @ And(
                EqualTo(
                  KnownFloatingPointNormalized(
                    NormalizeNaNAndZero(ar1 @ AttributeReference(_, _, _, _))
                  ),
                  KnownFloatingPointNormalized(
                    NormalizeNaNAndZero(ar2 @ AttributeReference(_, _, _, _))
                  )
                ),
                EqualTo(ar3 @ AttributeReference(_, _, _, _), ar4 @ AttributeReference(_, _, _, _))
              )
            ),
            _
          ) if JoinWithFunctionSupported =>
        sys.error("Join with function is not yet implemented")
      case j @ logical.Join(
            leftChild,
            rightChild,
            Inner,
            Some(
              condition @ EqualTo(
                ar1 @ AttributeReference(_, _, _, _),
                ar2 @ AttributeReference(_, _, _, _)
              )
            ),
            _
          ) =>
        val inputsLeft: List[CVector] =
          leftChild.output.toList.zipWithIndex.map { case (att, idx) =>
            sparkTypeToVeType(att.dataType).makeCVector(s"l_$InputPrefix$idx")
          }
        val inputsRight = rightChild.output.toList.zipWithIndex.map { case (att, idx) =>
          sparkTypeToVeType(att.dataType).makeCVector(s"r_$InputPrefix$idx")
        }

        val joins =
          List(
            GenericJoiner.Join(
              left = inputsLeft(
                leftChild.output.indexWhere(attr => List(ar1, ar2).exists(_.exprId == attr.exprId))
              ),
              right = inputsRight(
                rightChild.output.indexWhere(attr => List(ar1, ar2).exists(_.exprId == attr.exprId))
              )
            )
          )
        JoinMatcher(
          leftChild,
          rightChild,
          inputsLeft,
          inputsRight,
          GenericJoiner(
            inputsLeft = inputsLeft,
            inputsRight = inputsRight,
            joins = joins,
            outputs = (inputsLeft ++ inputsRight).map(cv => FilteredOutput(s"o_${cv.name}", cv))
          )
        )
    }
  }
}
