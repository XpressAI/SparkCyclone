package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.language.dynamics

//noinspection DuplicatedCode
final case class NativeAggregationEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  functionPrefix: String,
  partialFunction: CFunction,
  finalFunction: CFunction,
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = SinglePartition

  def collectInputRows(
    rows: Iterator[InternalRow],
    arrowSchema: org.apache.arrow.vector.types.pojo.Schema
  )(implicit allocator: BufferAllocator): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    rows.foreach { row =>
      arrowWriter.write(row)
    }
    arrowWriter.finish()
    root
  }

  def collectInputColBatches(columnarBatches: Iterator[ColumnarBatch], target: List[FieldVector])(
    implicit allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val root = new VectorSchemaRoot(target.asJava)
    val arrowWriter = ArrowWriter.create(root)

    for {
      columnarBatch <- columnarBatches
      i <- 0 until columnarBatch.numRows()
    } arrowWriter.write(columnarBatch.getRow(i))

    arrowWriter.finish()
    root
  }

  private def executeRowWise(): RDD[InternalRow] = {
    val partialFunctionName = s"${functionPrefix}_partial"
    val finalFunctionName = s"${functionPrefix}_final"

    val evaluator = nativeEvaluator.forCode(
      List(
        partialFunction.toCodeLines(partialFunctionName),
        finalFunction
          .toCodeLinesNoHeader(finalFunctionName)
      ).reduce(_ ++ _).lines.mkString("\n", "\n", "\n")
    )

    logger.debug(s"Will execute NewCEvaluationPlan for child ${child}; ${child.output}")

    child
      .execute()
      .mapPartitions { rows =>
        Iterator
          .continually {
            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)
            val timeZoneId = conf.sessionLocalTimeZone
            val root =
              collectInputRows(rows, ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId))
            val inputVectors = child.output.indices.map(root.getVector)
            val partialOutputVectors: List[ValueVector] =
              partialFunction.outputs.map(CFunctionGeneration.allocateFrom(_))

            try {

              val outputArgs = inputVectors.toList.map(_ => None) ++
                partialOutputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
              val inputArgs = inputVectors.toList.map(iv =>
                Some(SupportedVectorWrapper.wrapInput(iv))
              ) ++ partialOutputVectors.map(_ => None)

              evaluator.callFunction(
                name = partialFunctionName,
                inputArguments = inputArgs,
                outputArguments = outputArgs
              )

              new ColumnarBatch(
                partialOutputVectors.map(valueVector => new ArrowColumnVector(valueVector)).toArray,
                partialOutputVectors.head.getValueCount
              )
            } finally {
              inputVectors.foreach(_.close())
            }
          }
          .take(1)
      }
      .coalesce(numPartitions = 1, shuffle = false)
      .mapPartitions { iteratorColBatch =>
        Iterator
          .continually {

            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for final collector", 0, Long.MaxValue)
            val partialInputVectors: List[FieldVector] =
              finalFunction.inputs.map(CFunctionGeneration.allocateFrom(_))

            collectInputColBatches(iteratorColBatch, partialInputVectors)

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

            try {
              evaluator.callFunction(
                name = finalFunctionName,
                inputArguments = partialInputVectors.map(iv =>
                  Some(SupportedVectorWrapper.wrapInput(iv))
                ) ++ outputVectors.map(_ => None),
                outputArguments = finalFunction.inputs.map(_ => None) ++
                  outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
              )

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
            } finally {
              partialInputVectors.foreach(_.close())
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
