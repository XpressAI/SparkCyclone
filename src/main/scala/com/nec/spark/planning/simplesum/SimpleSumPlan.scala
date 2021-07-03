package com.nec.spark.planning.simplesum
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.simplesum.SimpleSumPlan.SumMethod
import com.nec.spark.planning.simplesum.SimpleSumPlan.SumMethod.NonCodegen
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed

object SimpleSumPlan {

  sealed trait SumMethod extends Serializable {
    def title: String

    def supportsCodegen: Boolean
    def needCopyResult: Boolean = false
    def needStopCheck: Boolean = false
    def limitNotReachedChecks: Seq[String] = Nil
    def canCheckLimitNotReached: Boolean = true
  }

  object SumMethod {
    sealed trait CodegenBased extends SumMethod {
      def initClass(simpleSumPlan: SimpleSumPlan): AnyRef
      override final def supportsCodegen: Boolean = true
      def consumerClass: Class[_]
    }
    object CodegenBased {
      case object JvmIncremental extends CodegenBased {
        override def title: String = "JVM Incremental (Codegen-based)"
        override def consumerClass: Class[_] = classOf[JvmUnsafeSummer]
        override def initClass(simpleSumPlan: SimpleSumPlan): AnyRef = new JvmUnsafeSummer
      }
      final case class ArrowCodegenBased(arrowSummer: ArrowSummer) extends CodegenBased {
        override def title: String = "JVM Arrow (Codegen-based)"
        override def consumerClass: Class[_] = classOf[ArrowUnsafeSummer]
        override def initClass(simpleSumPlan: SimpleSumPlan): AnyRef = {
          val timeZoneId = simpleSumPlan.conf.sessionLocalTimeZone
          val arrowSchema: Schema =
            ArrowUtilsExposed.toArrowSchema(simpleSumPlan.schema, timeZoneId)
          val allocator =
            ArrowUtilsExposed.rootAllocator.newChildAllocator(
              s"writer for a summing plan",
              0,
              Long.MaxValue
            )
          val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
          new ArrowUnsafeSummer(arrowSummer, root)
        }
      }
    }
    sealed trait NonCodegen extends SumMethod {
      override final def supportsCodegen: Boolean = false
    }
    object NonCodegen {
      case object RDDBased extends NonCodegen {
        override def title: String = "RDD Based"
      }
    }
  }
}

final case class SimpleSumPlan(child: SparkPlan, sumMethod: SimpleSumPlan.SumMethod)
  extends UnaryExecNode
  with CodegenSupport
  with SQLConfHelper {
  override def supportCodegen: Boolean = sumMethod.supportsCodegen
  protected def inputAttributes: Seq[Attribute] = child.output
  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()
  override def outputPartitioning: Partitioning = SinglePartition

  override protected def doExecute(): RDD[InternalRow] = {
    sumMethod match {
      case _: NonCodegen =>
        if (child.supportsColumnar)
          child
            .executeColumnar()
            .mapPartitions { columnarBatches =>
              Iterator
                .continually {
                  columnarBatches.map { colBatch =>
                    val theCol = colBatch.column(0)

                    (0 until colBatch.numRows()).view.map(idx => theCol.getDouble(idx)).sum
                  }.sum
                }
                .take(1)
            }
            .coalesce(numPartitions = 1)
            .mapPartitions(iterDoubles =>
              Iterator.continually(iterDoubles.sum).take(1).map { result =>
                val writer = new UnsafeRowWriter(1)
                writer.reset()
                writer.write(0, result)
                writer.getRow
              }
            )
        else
          child
            .execute()
            .mapPartitions(iterRows =>
              Iterator.continually(iterRows.map(row => row.getDouble(0)).sum).take(1)
            )
            .coalesce(numPartitions = 1)
            .mapPartitions(iterDoubles =>
              Iterator.continually(iterDoubles.sum).take(1).map { result =>
                val writer = new UnsafeRowWriter(1)
                writer.reset()
                writer.write(0, result)
                writer.getRow
              }
            )
      case _ => sys.error(s"Not supported for doExecute: ${sumMethod}")
    }
  }

  def createContainer: Object = sumMethod match {
    case cb: SumMethod.CodegenBased => cb.initClass(this)
    case other                      => sys.error(s"Cannot create a container for ${other}")
  }

  @transient private var containerVariable: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    sumMethod match {
      case cb: SumMethod.CodegenBased =>
        val executed = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "executed")
        val outputRow = ctx.freshName("outputRow")
        val thisPlan = ctx.addReferenceObj("plan", this)
        containerVariable = ctx.addMutableState(
          cb.consumerClass.getName,
          "batchProcessor",
          v => s"$v = (${cb.consumerClass.getName})$thisPlan.createContainer();",
          forceInline = true
        )
        ctx.INPUT_ROW = null
        val resultIterator = ctx.addMutableState(
          "scala.collection.Iterator<UnsafeRow>",
          "batchResultsIterator",
          forceInline = true
        )

        val doConsumeMethodIdx = ctx.freshName("doConsumeChild")
        val doConsumeFunctionName = ctx.addNewFunction(
          doConsumeMethodIdx,
          s"""
             |private void $doConsumeMethodIdx() throws java.io.IOException {
             |  // initialize aggregation buffer
             |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
             |}
       """.stripMargin
        )
        s"""
    if(!$executed) {
      $executed = true;
      $doConsumeFunctionName();
      $resultIterator = $containerVariable.execute();
    }
    while ($limitNotReachedCond $resultIterator.hasNext()) {
      UnsafeRow $outputRow = (UnsafeRow)$resultIterator.next();
      ${consume(ctx, null, outputRow)}
      if (shouldStop()) return;
    }
     """
      case other =>
        sys.error(s"Not supported for production: $other")
    }
  }
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    sumMethod match {
      case cb: SumMethod.CodegenBased =>
        s"""
       ${row.code}
       $containerVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
      case other =>
        sys.error(s"Not supported for production: $other")
    }
  }
}
