package com.nec.spark.planning

import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.ColumnarToRowTransition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed

import scala.language.dynamics
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper
import org.apache.arrow.vector.VarCharVector
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

final case class NewCEvaluationPlan(
  fName: String,
  resultExpressions: Seq[NamedExpression],
  lines: CodeLines,
  child: SparkPlan,
  inputReferenceNames: Set[String],
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with ColumnarToRowTransition
  with LazyLogging {

  override def output: Seq[Attribute] = resultExpressions.zipWithIndex.map { case (ne, idx) =>
    AttributeReference(name = s"value_${idx}", dataType = DoubleType, nullable = false)()
  }

  override def outputPartitioning: Partitioning = SinglePartition

  private def executeRowWise(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(lines.lines.mkString("\n", "\n", "\n"))
    child
      .execute()
      .mapPartitions { rows =>
        Iterator
          .continually {
            val timeZoneId = conf.sessionLocalTimeZone
            val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
              s"writer for word count",
              0,
              Long.MaxValue
            )
            val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
            val root = VectorSchemaRoot.create(arrowSchema, allocator)
            val arrowWriter = ArrowWriter.create(root)
            rows.foreach(row => arrowWriter.write(row))
            arrowWriter.finish()

            val inputVectors = child.output.map { attr =>
              root.getVector(0) match {
                case varCharVector: VarCharVector =>
                  varCharVector
              }
            }

            val outputVectors = resultExpressions
              .flatMap {
                case Alias(child, _) =>
                  child match {
                    case ae: AggregateExpression =>
                      ae.aggregateFunction.aggBufferAttributes
                    case other => List(other)
                  }
                case a @ AttributeReference(_, _, _, _) =>
                  List(a)
              }
              .zipWithIndex
              .map { case (ne, idx) =>
                ne.dataType match {
                  case StringType => new VarCharVector(s"out_${idx}", allocator)
                }
              }

            try {
              evaluator.callFunction(
                name = fName,
                inputArguments = inputVectors.toList.map(iv =>
                  Some(SupportedVectorWrapper.wrapVector(iv))
                ) ++ outputVectors.map(_ => None),
                outputArguments = inputVectors.toList.map(_ => None) ++
                  outputVectors.map(v => Some(SupportedVectorWrapper.wrapVector(v)))
              )
            } finally {
              inputVectors.foreach(_.close())
            }

            (0 until outputVectors.head.getValueCount).iterator.map { v_idx =>
              val writer = new UnsafeRowWriter(outputVectors.size)
              writer.reset()
              outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
                if (v_idx < v.getValueCount) {
                  v match {
                    case vector: VarCharVector =>
                      val bytes = vector.get(v_idx)
                      writer.write(c_idx, UTF8String.fromBytes(bytes))
                  }
                }
              }
              writer.getRow
            }

          }
          .take(1)
          .flatten
      }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    executeRowWise()
  }
}
