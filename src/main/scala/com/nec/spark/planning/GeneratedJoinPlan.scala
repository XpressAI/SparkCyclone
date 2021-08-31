package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.{BigIntVector, BitVectorHelper, Float8Vector, IntVector, VectorSchemaRoot}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{BinaryExecNode, ColumnarToRowTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.sql.util.ArrowUtilsExposed

case class GeneratedJoinPlan(
                              leftNode: SparkPlan,
                              rightNode: SparkPlan,
                              codeLines: CodeLines,
                              nativeEvaluator: NativeEvaluator,
                              inputs: Seq[Attribute],
                              resultExpressions: Seq[NamedExpression],
                              fName: String
                       ) extends BinaryExecNode
  with LazyLogging {
  trait JoinSideInput
  case object Left extends JoinSideInput
  case object Right extends JoinSideInput

  override def left: SparkPlan = leftNode

  override def right: SparkPlan = rightNode

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  private lazy val inputIndices = {
    val leftInputIds = left.output.zipWithIndex.map{
      case (expr, idx) => (expr.exprId, idx, Left)
    }
    val rightInputIds = right.output.zipWithIndex.map{
      case (expr, idx) => (expr.exprId, idx, Right)
    }
    val inputIds = leftInputIds ++ rightInputIds
    // We can technically replace that with flatMap, but let's keep that for now just to failfast if smth works different than expected.
    inputs
      .map(attr => inputIds.find(el => el._1 == attr.exprId))
      .map {
        case Some(data) => data
        case None => throw new RuntimeException("Input was not present in Left nor Right table!")
      }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(codeLines.lines.mkString("\n", "\n", "\n"))

    leftNode
      .execute()
      .coalesce(1)
      .zipPartitions(rightNode.execute().coalesce(1)) {
        case (leftIter, rightIter) => {
          val timeZoneId = conf.sessionLocalTimeZone
          val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
            s"writer for word count",
            0,
            Long.MaxValue
          )
          val leftArrowSchema = ArrowUtilsExposed.toArrowSchema(leftNode.schema, timeZoneId)
          val leftRoot = VectorSchemaRoot.create(leftArrowSchema, allocator)
          val leftArrowWriter = ArrowWriter.create(leftRoot)
          leftIter.foreach(row => leftArrowWriter.write(row))
          leftArrowWriter.finish()

          val rightArrowSchema = ArrowUtilsExposed.toArrowSchema(rightNode.schema, timeZoneId)
          val rightRoot = VectorSchemaRoot.create(rightArrowSchema, allocator)
          val rightArrowWriter = ArrowWriter.create(rightRoot)
          rightIter.foreach(row => rightArrowWriter.write(row))
          rightArrowWriter.finish()

          val inputVectors = inputIndices.map{
            case (_, idx, Left) => leftRoot.getVector(idx)
            case (_, idx, _) => rightRoot.getVector(idx)
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
                case DoubleType  => new Float8Vector(s"out_$idx", allocator)
                case LongType    => new BigIntVector(s"out_$idx", allocator)
                case IntegerType => new IntVector(s"out_$idx", allocator)
                case _           => new Float8Vector(s"out_$idx", allocator)
              }
            }

            try {
              evaluator.callFunction(
                name = fName,
                inputArguments = inputVectors.toList.map(iv =>
                  Some(SupportedVectorWrapper.wrapInput(iv))
                ) ++ outputVectors.map(_ => None),
                outputArguments = inputVectors.toList.map(_ => None) ++
                  outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
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
                  case vector: Float8Vector =>
                    val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if(isNull) writer.setNullAt(c_idx) else writer.write(c_idx, vector.get(v_idx))
                  case vector: IntVector =>
                    val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if(isNull) writer.setNullAt(c_idx) else writer.write(c_idx, vector.get(v_idx))
                  case vector: BigIntVector =>
                    val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if(isNull) writer.setNullAt(c_idx) else writer.write(c_idx, vector.get(v_idx))
                }
              }
            }
            writer.getRow
          }
        }
      }
  }
}

