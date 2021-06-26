package org.apache.spark.sql.execution.joins

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.BroadcastDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.joins.HashJoin.cast
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.ExplainUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.IntegralType
import org.apache.spark.sql.types.LongType

object HashedRelationInfo {

  /**
   * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
   *
   * If not, returns the original expressions.
   */
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    // TODO: support BooleanType, DateType and TimestampType
    if (
      keys.exists(!_.dataType.isInstanceOf[IntegralType])
      || keys.map(_.dataType.defaultSize).sum > 8
    ) {
      return keys
    }

    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      cast(keys.head, LongType)
    } else {
      keys.head
    }
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(
        ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(cast(e, LongType), Literal((1L << bits) - 1))
      )
    }
    keyExpr :: Nil
  }

  /**
   * Extract a given key which was previously packed in a long value using its index to
   * determine the number of bits to shift
   */
  def extractKeyExprAt(keys: Seq[Expression], index: Int): Expression = {
    // jump over keys that have a higher index value than the required key
    if (keys.size == 1) {
      assert(index == 0)
      cast(BoundReference(0, LongType, nullable = false), keys(index).dataType)
    } else {
      val shiftedBits =
        keys.slice(index + 1, keys.size).map(_.dataType.defaultSize * 8).sum
      val mask = (1L << (keys(index).dataType.defaultSize * 8)) - 1
      // build the schema for unpacking the required key
      cast(
        BitwiseAnd(
          ShiftRightUnsigned(BoundReference(0, LongType, nullable = false), Literal(shiftedBits)),
          Literal(mask)
        ),
        keys(index).dataType
      )
    }
  }
}

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
private[joins] case class HashedRelationInfo(
  relationTerm: String,
  keyIsUnique: Boolean,
  isEmpty: Boolean
)

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

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $buildSide ($opId)".trim
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }

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

  protected def buildSideKeyGenerator(): Projection =
    UnsafeProjection.create(buildBoundKeys)

  protected def streamSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(streamedBoundKeys)

  @transient protected[this] lazy val boundCondition: InternalRow => Boolean =
    if (condition.isDefined) {
      if (joinType == FullOuter && buildSide == BuildLeft) {
        // Put join left side before right side. This is to be consistent with
        // `ShuffledHashJoinExec.fullOuterJoin`.
        Predicate.create(condition.get, buildPlan.output ++ streamedPlan.output).eval _
      } else {
        Predicate.create(condition.get, streamedPlan.output ++ buildPlan.output).eval _
      }
    } else { (r: InternalRow) =>
      true
    }

  protected def createResultProjection(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output,
        (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true))
      )
  }

  private def innerJoin(
    streamIter: Iterator[InternalRow],
    hashedRelation: HashedRelation
  ): Iterator[InternalRow] = {
    val joinRow = new JoinedRow
    val joinKeys = streamSideKeyGenerator()

    if (hashedRelation == EmptyHashedRelation) {
      Iterator.empty
    } else if (hashedRelation.keyIsUnique) {
      streamIter.flatMap { srow =>
        joinRow.withLeft(srow)
        val matched = hashedRelation.getValue(joinKeys(srow))
        if (matched != null) {
          Some(joinRow.withRight(matched)).filter(boundCondition)
        } else {
          None
        }
      }
    } else {
      streamIter.flatMap { srow =>
        joinRow.withLeft(srow)
        val matches = hashedRelation.get(joinKeys(srow))
        if (matches != null) {
          matches.map(joinRow.withRight).filter(boundCondition)
        } else {
          Seq.empty
        }
      }
    }
  }

  protected def join(
    streamedIter: Iterator[InternalRow],
    hashed: HashedRelation,
    numOutputRows: SQLMetric
  ): Iterator[InternalRow] = {

    val joinedIter = innerJoin(streamedIter, hashed)

    val resultProj = createResultProjection
    joinedIter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key
   * has any null in it or not.
   */
  protected def genStreamSideJoinKey(
    ctx: CodegenContext,
    input: Seq[ExprCode]
  ): (ExprCode, String) = {
    ctx.currentVars = input
    if (streamedBoundKeys.length == 1 && streamedBoundKeys.head.dataType == LongType) {
      // generate the join key as Long
      val ev = streamedBoundKeys.head.genCode(ctx)
      (ev, ev.isNull)
    } else {
      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
      (ev, s"${ev.value}.anyNull()")
    }
  }

  /**
   * Generates the code for variable of build side.
   */
  private def genBuildSideVars(ctx: CodegenContext, matched: String): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    buildPlan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (joinType.isInstanceOf[InnerLike]) {
        ev
      } else {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val javaType = CodeGenerator.javaType(a.dataType)
        val code = code"""
                         |boolean $isNull = true;
                         |$javaType $value = ${CodeGenerator.defaultValue(a.dataType)};
                         |if ($matched != null) {
                         |  ${ev.code}
                         |  $isNull = ${ev.isNull};
                         |  $value = ${ev.value};
                         |}
         """.stripMargin
        ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType))
      }
    }
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows. This is used in Inner, Left Semi
   * and Left Anti joins.
   */
  protected def getJoinCondition(
    ctx: CodegenContext,
    input: Seq[ExprCode]
  ): (String, String, Seq[ExprCode]) = {
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = s"${ev.isNull} || !${ev.value}"
      s"""
         |$eval
         |${ev.code}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    (matched, checkCondition, buildVars)
  }

  /**
   * Generates the code for Inner join.
   */
  protected def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft  => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash inner join simply returns nothing.
      """.stripMargin
    } else if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |if ($matched != null) {
         |  $checkCondition {
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    } else {
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
         |      $numOutput.add(1);
         |      ${consume(ctx, resultVars)}
         |    }
         |  }
         |}
       """.stripMargin
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  )

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildBoundKeys, isNullAwareAntiJoin)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    sys.error("Expected WSCG to run")
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def needCopyResult: Boolean = true

  /**
   * Returns a tuple of Broadcast of HashedRelation and the variable name for it.
   */
  private def prepareBroadcast(ctx: CodegenContext): (Broadcast[HashedRelation], String) = {
    // create a name for HashedRelation
    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
    val clsName = broadcastRelation.value.getClass.getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(
      clsName,
      "relation",
      v => s"""
              | $v = (($clsName) $broadcast.value()).asReadOnlyCopy();
              | incPeakExecutionMemory($v.estimatedSize());
       """.stripMargin,
      forceInline = true
    )
    (broadcastRelation, relationTerm)
  }

  protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    HashedRelationInfo(
      relationTerm,
      broadcastRelation.value.keyIsUnique,
      broadcastRelation.value == EmptyHashedRelation
    )
  }

}
