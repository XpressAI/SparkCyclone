package com.nec.spark.agile.wscg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.execution.BlockingOperatorWithCodegen
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan

trait UnsafeExternalProcessorBase { this: SparkPlan with BlockingOperatorWithCodegen =>

  def child: SparkPlan

  override def supportsColumnar: Boolean = false

  override protected def doExecute(): RDD[InternalRow] = {
    sys.error("This should not be called if in WSCG")
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  type ContainerType <: UnsafeBatchProcessor

  def containerClass: Class[ContainerType]

  def createContainer(): ContainerType

  override protected def doProduce(ctx: CodegenContext): String = {
    val excecuted = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "_gen_executed")
    val outputRow = ctx.freshName("_gen_outputRow")
    val thisPlan = ctx.addReferenceObj("_gen_plan", this)
    containerVariable = ctx.addMutableState(
      containerClass.getName,
      "_gen_batchProcessor",
      v => s"$v = $thisPlan.createContainer();",
      forceInline = true
    )
    ctx.INPUT_ROW = null
    val resultIterator = ctx.addMutableState(
      "scala.collection.Iterator<UnsafeRow>",
      "_gen_usResultsIterator",
      forceInline = true
    )
    s"""
  if(!$excecuted) {
    $excecuted = true;
    ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
   $resultIterator = $containerVariable.execute();
   }

 while ($limitNotReachedCond $resultIterator.hasNext()) {
   UnsafeRow $outputRow = (UnsafeRow)$resultIterator.next();
   ${consume(ctx, null, outputRow)}
   if (shouldStop()) return;
}
   """
  }

  private var containerVariable: String = _

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    s"""
       |${row.code}
       |$containerVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
  }

}
