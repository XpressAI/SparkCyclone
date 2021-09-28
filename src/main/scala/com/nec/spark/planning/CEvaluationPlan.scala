package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterface
import com.nec.native.NativeEvaluator
import com.nec.spark.ColumnarBatchToArrow
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.planning.CEvaluationPlan.batchColumnarBatches
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.{BigIntVector, BitVectorHelper, Float8Vector, IntVector, SmallIntVector, VectorSchemaRoot}
import org.apache.commons.lang3.reflect.FieldUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.ColumnarToRowTransition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, ShortType}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import scala.collection.immutable
import scala.language.dynamics

import org.apache.spark.sql.catalyst.expressions.aggregate.Corr
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import org.apache.parquet.format.IntType
object CEvaluationPlan {

  object HasFloat8Vector {
    final class PrivateReader(val obj: Object) extends Dynamic {
      def selectDynamic(name: String): PrivateReader = {
        val clz = obj.getClass
        val field = FieldUtils.getAllFields(clz).find(_.getName == name) match {
          case Some(f) => f
          case None    => throw new NoSuchFieldException(s"Class $clz does not seem to have $name")
        }
        field.setAccessible(true)
        new PrivateReader(field.get(obj))
      }
    }

    implicit class RichObject(obj: Object) {
      def readPrivate: PrivateReader = new PrivateReader(obj)
    }
    def unapply(arrowColumnVector: ArrowColumnVector): Option[Float8Vector] = {
      PartialFunction.condOpt(arrowColumnVector.readPrivate.accessor.vector.obj) {
        case fv: Float8Vector => fv
      }
    }
  }

  val batchColumnarBatches = "spark.com.nec.spark.batch-batches"

}
final case class CEvaluationPlan(
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

  protected def inputVectorsIds: Seq[Int] = child.output.indices

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

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

            val inputVectors = inputVectorsIds
              .map(id => root.getVector(id))
            arrowWriter.finish()

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
                  case ShortType => new SmallIntVector(s"out_$idx", allocator)
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
                      if (isNull) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: IntVector =>
                      val isNull =
                        BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                      if (isNull) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: BigIntVector =>
                      val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                      if (isNull) writer.setNullAt(c_idx)
                      else writer.write(c_idx, vector.get(v_idx))
                    case vector: SmallIntVector =>
                      val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                      if (isNull) writer.setNullAt(c_idx)
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
      .coalesce(numPartitions = 1, shuffle = true)
      .mapPartitions { unsafeRows =>
        Iterator
          .continually {
            val unsafeRowsList = unsafeRows.toList
            val isAggregation = resultExpressions.exists {
              case Alias(child, _) => child.isInstanceOf[AggregateExpression]
              case _               => false
            }

            if (isAggregation) {
              val startingIndices = resultExpressions.view
                .flatMap {
                  case ne @ Alias(
                        AggregateExpression(aggregateFunction, mode, isDistinct, filter, resultId),
                        name
                      ) =>
                    aggregateFunction.aggBufferAttributes.map(attr => ne)
                }
                .zipWithIndex
                .groupBy(_._1)
                .mapValues(_.map(_._2).min)

              /** total Aggregation */
              val writer = new UnsafeRowWriter(resultExpressions.size)
              writer.reset()

              resultExpressions.view.zipWithIndex.foreach {
                case (a @ Alias(AggregateExpression(Average(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val sum = unsafeRowsList.map(_.getDouble(idx)).sum
                  val count = unsafeRowsList.map(_.getLong(idx + 1)).sum
                  val result = sum / count
                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Sum(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val result = unsafeRowsList.map(_.getDouble(idx)).sum
                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Count(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val result = unsafeRowsList.map(_.getInt(idx)).sum

                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Corr(_, _, _), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val corrSum = unsafeRowsList.map(_.getDouble(idx)).sum
                  val count = unsafeRowsList.size
                  val result = corrSum / count
                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Min(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val result = unsafeRowsList.map(_.getDouble(idx)).min
                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Max(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val result = unsafeRowsList.map(_.getDouble(idx)).max
                  writer.write(outIdx, result)
                case other => sys.error(s"Other not supported: $other")
              }
              Iterator(writer.getRow)
            } else unsafeRowsList.iterator
          }
          .take(1)
          .flatten
      }
  }

  private def executeColumnWise(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(lines.lines.mkString("\n", "\n", "\n"))
    val maybeBatch = Option(sparkContext.getConf.getInt(batchColumnarBatches, 0)).filter(_ > 1)
    maybeBatch match {
      case Some(batchBatchSize) =>
        child
          .executeColumnar()
          .mapPartitions(iteratorBatches =>
            iteratorBatches
              .grouped(batchBatchSize)
              .flatMap(seqBatch => executeColumnarPerBatch(evaluator, seqBatch: _*))
          )
          .coalesce(numPartitions = 1, shuffle = true)
          .mapPartitions(unsafeRows => reduceRows(unsafeRows))
      case _ =>
        child
          .executeColumnar()
          .flatMap(colBatch => executeColumnarPerBatch(evaluator, colBatch))
          .coalesce(numPartitions = 1, shuffle = true)
          .mapPartitions(unsafeRows => reduceRows(unsafeRows))
    }
  }

  private def reduceRows(unsafeRows: Iterator[UnsafeRow]) = {
    Iterator
      .continually {
        val unsafeRowsList = unsafeRows.toList
        val isAggregation =
          resultExpressions.exists {
            case Alias(child, name) => child.isInstanceOf[AggregateExpression]
            case _                  => false
          }

        if (isAggregation) {
          val startingIndices = resultExpressions.view
            .flatMap {
              case ne @ Alias(
                    AggregateExpression(aggregateFunction, mode, isDistinct, filter, resultId),
                    name
                  ) =>
                aggregateFunction.aggBufferAttributes.map(attr => ne)
            }
            .zipWithIndex
            .groupBy(_._1)
            .mapValues(_.map(_._2).min)

          /** total Aggregation */
          val writer = new UnsafeRowWriter(resultExpressions.size)
          writer.reset()

          resultExpressions.view.zipWithIndex.foreach {
            case (a @ Alias(AggregateExpression(Average(_), _, _, _, _), _), outIdx) =>
              val idx = startingIndices(a)
              val sum = unsafeRowsList.map(_.getDouble(idx)).sum
              val count = unsafeRowsList.map(_.getLong(idx + 1)).sum
              val result = sum / count
              writer.write(outIdx, result)
            case (a @ Alias(AggregateExpression(Sum(_), _, _, _, _), _), outIdx) =>
              val idx = startingIndices(a)
              val result = unsafeRowsList.map(_.getDouble(idx)).sum
              writer.write(outIdx, result)
            case (a @ Alias(AggregateExpression(Count(_), _, _, _, _), _), outIdx) =>
              val idx = startingIndices(a)
              val result = unsafeRowsList.map(_.getInt(idx)).sum
              writer.write(outIdx, result)
            case (a @ Alias(AggregateExpression(Corr(_, _, _), _, _, _, _), _), outIdx) =>
              val idx = startingIndices(a)
              val corrSum = unsafeRowsList.map(_.getDouble(idx)).sum
              val count = unsafeRowsList.size
              val result = corrSum / count
              writer.write(outIdx, result)
            case (a @ Alias(AggregateExpression(Min(_), _, _, _, _), _), outIdx) =>
              val idx = startingIndices(a)
              val result = unsafeRowsList.map(_.getDouble(idx)).min
              writer.write(outIdx, result)
            case (a @ Alias(AggregateExpression(Max(_), _, _, _, _), _), outIdx) =>
              val idx = startingIndices(a)
              val result = unsafeRowsList.map(_.getDouble(idx)).max
              writer.write(outIdx, result)
            case other => sys.error(s"Other not supported: $other")
          }
          Iterator(writer.getRow)
        } else unsafeRowsList.iterator
      }
      .take(1)
      .flatten
  }
  private def executeColumnarPerBatch(
    evaluator: ArrowNativeInterface,
    columnarBatch: ColumnarBatch*
  ): immutable.IndexedSeq[UnsafeRow] = {
    val uuid = java.util.UUID.randomUUID()
    logger.debug(s"[$uuid] Starting evaluation of a columnar batch...")
    val batchStartTime = System.currentTimeMillis()
    val timeZoneId = conf.sessionLocalTimeZone
    val allocatorIn =
      ArrowUtilsExposed.rootAllocator.newChildAllocator(s"create input data", 0, Long.MaxValue)
    val allocatorOut =
      ArrowUtilsExposed.rootAllocator.newChildAllocator(s"create output data", 0, Long.MaxValue)
    val outputVectors = resultExpressions
      .flatMap({
        case Alias(child, name) =>
          child match {
            case ae: AggregateExpression =>
              ae.aggregateFunction.aggBufferAttributes
            case other => List(other)
          }
        case other => List(other)
      })
      .zipWithIndex
      .map { case (ne, idx) =>
        ne.dataType match {
          case DoubleType  => new Float8Vector(s"out_$idx", allocatorOut)
          case LongType    => new BigIntVector(s"out_$idx", allocatorOut)
          case IntegerType => new IntVector(s"out_$idx", allocatorOut)
          case _           => new Float8Vector(s"out_$idx", allocatorOut)
        }
      }
    logger.debug(s"[$uuid] allocated output vectors")
    try {
      val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
      logger.debug(
        s"[$uuid] loading input vectors - there are ${columnarBatch.map(_.numRows()).sum} rows of data (${columnarBatch
          .map(_.numRows())})"
      )
      val (inputVectorSchemaRoot, inputVectors) =
        ColumnarBatchToArrow.fromBatch(arrowSchema, allocatorIn)(columnarBatch: _*)
      logger.debug(s"[$uuid] loaded input vectors.")
      val clearedInputCols: Int = (0 until columnarBatch.head.numCols()).view
        .flatMap { colNo =>
          columnarBatch.map(_.column(colNo))
        }
        .collect { case acv: ArrowColumnVector => acv }
        .count(avc => { avc.close(); true })
      logger.debug(s"[$uuid] cleared $clearedInputCols input cols.")
      try {
        logger.debug(s"[$uuid] executing the function 'f'.")

        evaluator.callFunction(
          name = fName,
          inputArguments = inputVectors.map(iv => Some(iv)) ++ outputVectors.map(_ => None),
          outputArguments = inputVectors.map(_ => None) ++
            outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
        )
        logger.debug(s"[$uuid] executed the function 'f'.")
      } finally {
        inputVectorSchemaRoot.close()
        logger.debug(s"[$uuid] cleared input vectors")
      }
    } finally allocatorIn.close()

    logger.debug(s"[$uuid] preparing transfer to UnsafeRows...")
    val writer = new UnsafeRowWriter(outputVectors.size)
    writer.reset()
    logger.debug(s"[$uuid] received ${outputVectors.head.getValueCount} items from VE.")

    val result =
      try {
        (0 until outputVectors.head.getValueCount).map { v_idx =>
          outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
            if (v_idx < v.getValueCount) {
              v match {
                case vector: Float8Vector =>
                  val isNull =
                    BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                  if (isNull) writer.setNullAt(c_idx)
                  else writer.write(c_idx, vector.get(v_idx))
                case vector: IntVector =>
                  val isNull =
                    BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                  if (isNull) writer.setNullAt(c_idx)
                  else writer.write(c_idx, vector.get(v_idx))
                case vector: BigIntVector =>
                  val isNull =
                    BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                  if (isNull) writer.setNullAt(c_idx)
                  else writer.write(c_idx, vector.get(v_idx))
                case _ =>
              }
            }
          }
          writer.getRow.copy()
        }
      } finally {
        outputVectors.foreach(_.close())
        allocatorOut.close()
      }

    logger.debug(s"[$uuid] completed transfer.")
    logger.debug(
      s"[$uuid] Evaluation of batch took ${System.currentTimeMillis() - batchStartTime}ms."
    )

    result
  }
  override protected def doExecute(): RDD[InternalRow] = {
    if (child.supportsColumnar) executeColumnWise() else executeRowWise()
  }
}
