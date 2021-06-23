package com.nec.spark.agile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan

final case class IdentityCodegenPlan(child: SparkPlan) extends SparkPlan with CodegenSupport {
  override protected def doExecute(): RDD[InternalRow] = sys.error("This should not be called.")
  override def output: Seq[Attribute] = child.output
  override def children: Seq[SparkPlan] = Seq(child)
  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(child.execute())
  override protected def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val resultVars = input.zipWithIndex.map { case (ev, i) => ev }
    s"""
       |do {
       |  ${consume(ctx, resultVars)}
       |} while(false);
     """.stripMargin
  }

}
