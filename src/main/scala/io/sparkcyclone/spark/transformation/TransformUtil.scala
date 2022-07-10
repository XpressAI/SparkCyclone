package io.sparkcyclone.spark.transformation

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

object TransformUtil {
  implicit final class RichTreeNode(namedExpression: NamedExpression) {
    def transformSelf(rule: PartialFunction[Expression, Expression]): NamedExpression =
      namedExpression.transform(rule).asInstanceOf[NamedExpression]
  }
}
