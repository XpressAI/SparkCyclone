package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.BroadcastDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
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

  protected lazy val (buildKeys, streamedKeys) = {
    require(
      leftKeys.length == rightKeys.length &&
        leftKeys
          .map(_.dataType)
          .zip(rightKeys.map(_.dataType))
          .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types"
    )
    buildSide match {
      case BuildLeft  => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  @transient protected lazy val (buildOutput, streamedOutput) = {
    buildSide match {
      case BuildLeft  => (left.output, right.output)
      case BuildRight => (right.output, left.output)
    }
  }

  @transient protected lazy val buildBoundKeys: Seq[Expression] =
    bindReferences(HashJoin.rewriteKeyExpr(buildKeys), buildOutput)

  @transient protected lazy val streamedBoundKeys: Seq[Expression] =
    bindReferences(HashJoin.rewriteKeyExpr(streamedKeys), streamedOutput)

  protected def streamSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(streamedBoundKeys)

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
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
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

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildBoundKeys, isNullAwareAntiJoin)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  protected override def doExecute(): RDD[InternalRow] = sys.error("Expected WSCG to run")

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()

  override def needCopyResult: Boolean = true

}
