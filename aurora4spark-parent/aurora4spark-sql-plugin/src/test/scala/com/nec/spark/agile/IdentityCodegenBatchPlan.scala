package com.nec.spark.agile

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.BlockingOperatorWithCodegen
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan

final case class IdentityCodegenBatchPlan(child: SparkPlan)
  extends SparkPlan
  with BlockingOperatorWithCodegen {
  type ContainerType = UnsafeExternalDuplicator
  def createContainer(): UnsafeExternalDuplicator = new UnsafeExternalDuplicator
  override def children: Seq[SparkPlan] = Seq(child)

  protected def inputAttributes: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] =
    sys.error("This should not be called if in WSCG")

  override def output: Seq[Attribute] = child.output

  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doProduce(ctx: CodegenContext): String = {
    val executed = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "executed")
    val outputRow = ctx.freshName("outputRow")
    val thisPlan = ctx.addReferenceObj("plan", this)
    containerVariable = ctx.addMutableState(
      classOf[UnsafeExternalDuplicator].getName,
      "batchProcessor",
      v => s"$v = $thisPlan.createContainer();",
      forceInline = true
    )
    ctx.INPUT_ROW = null
    val resultIterator = ctx.addMutableState(
      "scala.collection.Iterator<UnsafeRow>",
      "batchResultsIterator",
      forceInline = true
    )

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    val doAggFuncName = ctx.addNewFunction(
      doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  // initialize aggregation buffer
         |  System.out.println("Doing a produce here in Batch Planner...");
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         |}
       """.stripMargin
    )

    s"""
    if(!$executed) {
      $executed = true;
      $doAggFuncName();
      $resultIterator = $containerVariable.execute();
    }
    while ($limitNotReachedCond $resultIterator.hasNext()) {
      UnsafeRow $outputRow = (UnsafeRow)$resultIterator.next();
      ${consume(ctx, null, outputRow)}
      if (shouldStop()) return;
    }
     """
  }

  @transient private var containerVariable: String = _

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       ${row.code}
       $containerVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
  }

}
