package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.StructType

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class OurMinimalHashAggregateExec(
  requiredChildDistributionExpressions: Option[Seq[Expression]],
  groupingExpressions: Seq[NamedExpression],
  aggregateExpressions: Seq[AggregateExpression],
  aggregateAttributes: Seq[Attribute],
  initialInputBufferOffset: Int,
  resultExpressions: Seq[NamedExpression],
  child: SparkPlan
) extends BlockingOperatorWithCodegen
  with UnaryExecNode {

  protected def inputAttributes: Seq[Attribute] = child.output

  private val inputAggBufferAttributes: Seq[Attribute] = {
    aggregateExpressions
      // there're exactly four cases needs `inputAggBufferAttributes` from child according to the
      // agg planning in `AggUtils`: Partial -> Final, PartialMerge -> Final,
      // Partial -> PartialMerge, PartialMerge -> PartialMerge.
      .filter(a => a.mode == Final || a.mode == PartialMerge)
      .flatMap(_.aggregateFunction.inputAggBufferAttributes)
  }

  protected val aggregateBufferAttributes: Seq[AttributeReference] =
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes) ++
      // it's not empty when the inputAggBufferAttributes is not equal to the aggregate buffer
      // attributes of the child Aggregate, when the child Aggregate contains the subquery in
      // AggregateFunction. See SPARK-31620 for more details.
      AttributeSet(inputAggBufferAttributes.filterNot(child.output.contains))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs)                  => ClusteredDistribution(exprs) :: Nil
      case None                         => UnspecifiedDistribution :: Nil
    }
  }
  require(OurMinimalHashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  protected override def doExecute(): RDD[InternalRow] = sys.error("Should not be called here")

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = {
    // ImperativeAggregate and filter predicate are not supported right now
    // TODO: SPARK-30027 Support codegen for filter exprs in OurMinimalHashAggregateExec
    !(aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate]) ||
      aggregateExpressions.exists(_.filter.isDefined))
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  protected override def doProduce(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")
    // The generated function doesn't have input row in the code context.
    ctx.INPUT_ROW = null

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.map(f => f.initialValues)
    bufVars = initExpr.map { exprs =>
      exprs.map { e =>
        val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "bufIsNull")
        val value = ctx.addMutableState(CodeGenerator.javaType(e.dataType), "bufValue")
        // The initial expression should not access any column
        val ev = e.genCode(ctx)
        val initVars = code"""
                             |$isNull = ${ev.isNull};
                             |$value = ${ev.value};
         """.stripMargin
        ExprCode(
          ev.code + initVars,
          JavaCode.isNullGlobal(isNull),
          JavaCode.global(value, e.dataType)
        )
      }
    }
    val flatBufVars = bufVars.flatten
    val initBufVar = evaluateVariables(flatBufVars)

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    val doAggFuncName = ctx.addNewFunction(
      doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  // initialize aggregation buffer
         |  $initBufVar
         |
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         |}
       """.stripMargin
    )

    s"""
       |while (!$initAgg) {
       |  $initAgg = true;
       |  $doAggFuncName();
       |
       |  ${consume(ctx, flatBufVars).trim}
       |}
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ inputAttributes
    // To individually generate code for each aggregate function, an element in `updateExprs` holds
    // all the expressions for the buffer of an aggregation function.
    val updateExprs = aggregateExpressions.map { e =>
      e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
    }
    ctx.currentVars = bufVars.flatten ++ input
    val boundUpdateExprs = updateExprs.map { updateExprsForOneFunc =>
      bindReferences(updateExprsForOneFunc, inputAttrs)
    }
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExprs.flatten)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val bufferEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
      ctx.withSubExprEliminationExprs(subExprs.states) {
        boundUpdateExprsForOneFunc.map(_.genCode(ctx))
      }
    }

    val aggNames = functions.map(_.prettyName)
    val aggCodeBlocks = bufferEvals.zipWithIndex.map { case (bufferEvalsForOneFunc, i) =>
      val bufVarsForOneFunc = bufVars(i)
      // All the update code for aggregation buffers should be placed in the end
      // of each aggregation function code.
      val updates = bufferEvalsForOneFunc.zip(bufVarsForOneFunc).map { case (ev, bufVar) =>
        s"""
           |${bufVar.isNull} = ${ev.isNull};
           |${bufVar.value} = ${ev.value};
         """.stripMargin
      }
      code"""
            |${ctx.registerComment(s"do aggregate for ${aggNames(i)}")}
            |${ctx.registerComment("evaluate aggregate function")}
            |${evaluateVariables(bufferEvalsForOneFunc)}
            |${ctx.registerComment("update aggregation buffers")}
            |${updates.mkString("\n").trim}
       """.stripMargin
    }

    val codeToEvalAggFunc =
      splitAggregateExpressions(ctx, aggNames, boundUpdateExprs, aggCodeBlocks, subExprs.states)

    s"""
       |// do aggregate
       |// common sub-expressions
       |$effectiveCodes
       |// evaluate aggregate functions and update aggregation buffers
       |$codeToEvalAggFunc
     """.stripMargin
  }

  // The variables are used as aggregation buffers and each aggregate function has one or more
  // ExprCode to initialize its buffer slots. Only used for aggregation without keys.
  private var bufVars: Seq[Seq[ExprCode]] = _

  // Splits aggregate code into small functions because the most of JVM implementations
  // can not compile too long functions. Returns None if we are not able to split the given code.
  //
  // Note: The difference from `CodeGenerator.splitExpressions` is that we define an individual
  // function for each aggregation function (e.g., SUM and AVG). For example, in a query
  // `SELECT SUM(a), AVG(a) FROM VALUES(1) t(a)`, we define two functions
  // for `SUM(a)` and `AVG(a)`.
  private def splitAggregateExpressions(
    ctx: CodegenContext,
    aggNames: Seq[String],
    aggBufferUpdatingExprs: Seq[Seq[Expression]],
    aggCodeBlocks: Seq[Block],
    subExprs: Map[Expression, SubExprEliminationState]
  ): String = {
    val splitCodes = aggBufferUpdatingExprs.zipWithIndex.map { case (vv, i) =>
      val args =
        vv.map(CodeGenerator.getLocalInputVariableValues(ctx, _, subExprs)._1)
          .reduce(_ ++ _)
          .toSeq
      val argList = args
        .map { v =>
          s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}"
        }
        .mkString(", ")
      val doAggFunc = ctx.freshName(s"doAggregate_${aggNames(i)}")
      val doAggFuncName = ctx.addNewFunction(
        doAggFunc,
        s"""
           |private void $doAggFunc($argList) throws java.io.IOException {
           |  ${aggCodeBlocks(i)}
           |}
             """.stripMargin
      )

      val inputVariables = args.map(_.variableName).mkString(", ")
      s"$doAggFuncName($inputVariables);"
    }
    splitCodes.mkString("\n").trim
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions
    .map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

  /**
   * This is called by generated Java class, should be public.
   */
  def createHashMap(): UnsafeFixedWidthAggregationMap = {
    // create initialized aggregate buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)

    // create hashMap
    new UnsafeFixedWidthAggregationMap(
      initialBuffer,
      bufferSchema,
      groupingKeySchema,
      TaskContext.get(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes
    )
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }
}

object OurMinimalHashAggregateExec {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
