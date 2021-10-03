package com.nec.spark.planning

import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  VarCharVector,
  VectorSchemaRoot
}
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
import org.apache.spark.sql.execution.{
  ColumnarToRowExec,
  ColumnarToRowTransition,
  SparkPlan,
  UnaryExecNode
}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  IntegerType,
  LongType,
  ShortType,
  StringType
}
import org.apache.spark.sql.util.ArrowUtilsExposed

import scala.language.dynamics
import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.spark.agile.CFunctionGeneration.CFunction
import org.apache.spark.unsafe.types.UTF8String

final case class NativeAggregationEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  functionPrefix: String,
  partialFunction: CFunction,
  finalFunction: CFunction,
  child: SparkPlan,
  inputReferenceNames: Set[String],
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = SinglePartition

  private def executeRowWise(): RDD[InternalRow] = {

    val partialFunctionName = s"${functionPrefix}_partial"
    val finalFunctionName = s"${functionPrefix}_final"

    val evaluator = nativeEvaluator.forCode(
      (
        partialFunction.toCodeLines(partialFunctionName) ++ finalFunction
          .toCodeLines(finalFunctionName)
      ).lines.mkString("\n", "\n", "\n")
    )
    logger.debug(s"Will execute NewCEvaluationPlan for child ${child}; ${child.output}")
    child
      .execute()
      // for aggregations, so far, we need this
      .coalesce(numPartitions = 1, shuffle = false)
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
            rows.foreach { row =>
              arrowWriter.write(row)
            }
            arrowWriter.finish()

            val inputVectors = child.output.zipWithIndex.map { case (attr, idx) =>
              root.getVector(idx) match {
                case varCharVector: VarCharVector =>
                  varCharVector
                case float8Vector: Float8Vector =>
                  float8Vector
                case intVector: IntVector =>
                  intVector
                case bigIntVector: BigIntVector =>
                  bigIntVector
                case smallIntVector: SmallIntVector =>
                  smallIntVector
                case bitVector: BitVector =>
                  bitVector
              }
            }

            val outputVectors = outputExpressions
              .flatMap {
                case Alias(child, _) =>
                  child match {
                    // disabled for group-by integration
//                    case ae: AggregateExpression =>
//                      ae.aggregateFunction.aggBufferAttributes
                    case other => List(other)
                  }
                case a @ AttributeReference(_, _, _, _) =>
                  List(a)
              }
              .zipWithIndex
              .map { case (ne, idx) =>
                ne.dataType match {
                  case StringType  => new VarCharVector(s"out_${idx}", allocator)
                  case LongType    => new BigIntVector(s"out_${idx}", allocator)
                  case IntegerType => new IntVector(s"out_${idx}", allocator)
                  case ShortType   => new SmallIntVector(s"out_${idx}", allocator)
                  case DoubleType  => new Float8Vector(s"out_${idx}", allocator)
                  case BooleanType => new BitVector(s"out_${idx}", allocator)
                }
              }

            val outputArgs = inputVectors.toList.map(_ => None) ++
              outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))

            try {
              evaluator.callFunction(
                name = partialFunctionName,
                inputArguments = inputVectors.toList.map(iv =>
                  Some(SupportedVectorWrapper.wrapInput(iv))
                ) ++ outputVectors.map(_ => None),
                outputArguments = outputArgs
              )
            } finally {
              inputVectors.foreach(_.close())
            }
            val cnt = outputVectors.head.getValueCount
            logger.info(s"Got ${cnt} results back; ${outputVectors}")
            (0 until cnt).iterator.map { v_idx =>
              val writer = new UnsafeRowWriter(outputVectors.size)
              writer.reset()
              outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
                if (v_idx < v.getValueCount) {
                  v match {
                    case vector: VarCharVector =>
                      if (vector.isNull(v_idx)) writer.setNullAt(c_idx)
                      else {
                        val bytes = vector.get(v_idx)
                        writer.write(c_idx, UTF8String.fromBytes(bytes))
                      }
                    case vector: Float8Vector =>
                      if (vector.isNull(v_idx)) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: IntVector =>
                      if (vector.isNull(v_idx)) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: BigIntVector =>
                      if (vector.isNull(v_idx)) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: SmallIntVector =>
                      if (vector.isNull(v_idx)) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: BitVector =>
                      if (vector.isNull(v_idx)) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
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
