package org.apache.spark.sql.execution.joins

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics

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
) extends OurHashJoin {

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

  override def needCopyResult: Boolean =true

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

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    HashedRelationInfo(
      relationTerm,
      broadcastRelation.value.keyIsUnique,
      broadcastRelation.value == EmptyHashedRelation
    )
  }

}
