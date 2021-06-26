package com.nec.spark.cgescape

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

/**
 * Example of how to do dead-simple identity codegen that simply returns the same thing.
 *
 * This one is streaming - we are not too likely to use it however it is important to keep the dead-minimum example
 * for reference, as it is quite the feat to reverse-engineer Spark's codegen plans as they are highly complex,
 * for example 1000k+ lines of code.
 * */
final case class IdentityCodegenPlan(child: SparkPlan) extends UnaryExecNode with CodegenSupport {
  override protected def doExecute(): RDD[InternalRow] = sys.error("This should not be called for a codegen-only plan")
  override def output: Seq[Attribute] = child.output
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()
  protected override def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |do {
       |  ${consume(ctx, input)}
       |} while(false);
     """.stripMargin
  }
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
}
