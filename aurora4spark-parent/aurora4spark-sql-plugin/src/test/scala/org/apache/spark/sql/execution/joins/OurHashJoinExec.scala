package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.CodegenSupport

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */

case class OurHashJoinExec(
  leftKeys: Seq[Expression],
  rightKeys: Seq[Expression],
  joinType: JoinType,
  buildSide: BuildSide,
  condition: Option[Expression],
  left: SparkPlan,
  right: SparkPlan,
  isNullAwareAntiJoin: Boolean = false
) extends BaseJoinExec
  with CodegenSupport {

  override def output: Seq[Attribute] = left.output ++ right.output

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft  => (left, right)
    case BuildRight => (right, left)
  }

  override def doProduce(ctx: CodegenContext): String =
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val relationTerm = {
      // create a name for HashedRelation
      val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
      val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
      val clsName = broadcastRelation.value.getClass.getName

      // Inline mutable state since not many join operations in a task
      ctx.addMutableState(
        clsName,
        "relation",
        v => s"""
                | $v = (($clsName) $broadcast.value()).asReadOnlyCopy();
                | incPeakExecutionMemory($v.estimatedSize());
       """.stripMargin,
        forceInline = true
      )
    }
    val (keyEv, anyNull) = {
      ctx.currentVars = input

      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(
        ctx,
        bindReferences(
          HashJoin.rewriteKeyExpr(buildSide match {
            case BuildLeft  => rightKeys
            case BuildRight => leftKeys
          }),
          buildSide match {
            case BuildLeft  => right.output
            case BuildRight => left.output
          }
        )
      )
      (ev, s"${ev.value}.anyNull()")
    }
    val matched = ctx.freshName("matched")
    val checkCondition = ""
    val buildVars = {
      ctx.currentVars = null
      ctx.INPUT_ROW = matched
      buildPlan.output.zipWithIndex.map { case (a, i) =>
        BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      }
    }
    val resultVars = buildSide match {
      case BuildLeft  => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName

    s"""
       |// generate join key for stream side
       |${keyEv.code}
       |// find matches from HashRelation
       |$iteratorCls $matches = $anyNull ?
       |  null : ($iteratorCls)$relationTerm.get(${keyEv.value});
       |if ($matches != null) {
       |  while ($matches.hasNext()) {
       |    UnsafeRow $matched = (UnsafeRow) $matches.next();
       |    $checkCondition {
       |      ${consume(ctx, resultVars)}
       |    }
       |  }
       |}
       """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = sys.error("Expected WSCG to run")

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()

  override def needCopyResult: Boolean = true

}
