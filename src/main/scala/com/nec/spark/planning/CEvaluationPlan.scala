package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import com.nec.native.NativeEvaluator
import com.nec.spark.ColumnarBatchToArrow
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.ColumnarToRowTransition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector

import scala.language.dynamics

object CEvaluationPlan {

  object HasFloat8Vector {
    final class PrivateReader(val obj: Object) extends Dynamic {
      def selectDynamic(name: String): PrivateReader = {
        val clz = obj.getClass
        val field = FieldUtils.getAllFields(clz).find(_.getName == name) match {
          case Some(f) => f
          case None    => throw new NoSuchFieldException(s"Class ${clz} does not seem to have ${name}")
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
}
final case class CEvaluationPlan(
  resultExpressions: Seq[NamedExpression],
  lines: CodeLines,
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with ColumnarToRowTransition
  with LazyLogging {

  protected def inputAttributes: Seq[Attribute] = child.output

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

            val inputVectors = inputAttributes.zipWithIndex.map { case (attr, idx) =>
              root.getVector(idx).asInstanceOf[Float8Vector]
            }
            arrowWriter.finish()

            val outputVectors = resultExpressions
              .flatMap(_.asInstanceOf[Alias].child match {
                case ae: AggregateExpression =>
                  ae.aggregateFunction.aggBufferAttributes
                case other => List(other)
              })
              .zipWithIndex
              .map { case (ne, idx) =>
                val outputVector = new Float8Vector(s"out_${idx}", allocator)
                outputVector
              }

            try {
              evaluator.callFunction(
                name = "f",
                inputArguments = inputVectors.toList.map(iv =>
                  Some(Float8VectorWrapper(iv))
                ) ++ outputVectors.map(_ => None),
                outputArguments = inputVectors.toList.map(_ => None) ++
                  outputVectors.map(v => Some(Float8VectorWrapper(v)))
              )
            } finally {
              inputVectors.foreach(_.close())
            }

            (0 until outputVectors.head.getValueCount).iterator.map { v_idx =>
              val writer = new UnsafeRowWriter(outputVectors.size)
              writer.reset()
              outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
                val doubleV = v.getValueAsDouble(v_idx)
                writer.write(c_idx, doubleV)
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
            val isAggregation = resultExpressions.exists(
              _.asInstanceOf[Alias].child.isInstanceOf[AggregateExpression]
            )

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
                  val count = unsafeRowsList.map(_.getDouble(idx + 1)).sum
                  val result = sum / count
                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Sum(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val result = unsafeRowsList.map(_.getDouble(idx)).sum
                  writer.write(outIdx, result)
                case other => sys.error(s"Other not supported: ${other}")
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
    child
      .executeColumnar()
      .flatMap { columnarBatch =>
        val timeZoneId = conf.sessionLocalTimeZone
        val allocatorIn =
          ArrowUtilsExposed.rootAllocator.newChildAllocator(s"create input data", 0, Long.MaxValue)
        val allocatorOut =
          ArrowUtilsExposed.rootAllocator.newChildAllocator(s"create output data", 0, Long.MaxValue)
        val uuid = java.util.UUID.randomUUID()
        val outputVectors = resultExpressions
          .flatMap(_.asInstanceOf[Alias].child match {
            case ae: AggregateExpression =>
              ae.aggregateFunction.aggBufferAttributes
            case other => List(other)
          })
          .zipWithIndex
          .map { case (ne, idx) =>
            new Float8Vector(s"out_${idx}", allocatorOut)
          }
        logger.debug(s"[$uuid] allocated output vectors")
        try {
          val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
          logger.debug(
            s"[$uuid] loading input vectors - there are ${columnarBatch.numRows()} rows of data"
          )
          val (inputVectorSchemaRoot, inputVectors) =
            ColumnarBatchToArrow.fromBatch(arrowSchema, allocatorIn)(columnarBatch)
          logger.debug(s"[$uuid] loaded input vectors.")
          val clearedInputCols: Int = (0 until columnarBatch.numCols()).view
            .map { colNo =>
              columnarBatch.column(colNo)
            }
            .collect { case acv: ArrowColumnVector => acv }
            .count(avc => { avc.close(); true })
          logger.debug(s"[$uuid] cleared ${clearedInputCols} input cols.")
          try {
            logger.debug(s"[$uuid] executing the function 'f'.")
            evaluator.callFunction(
              name = "f",
              inputArguments = inputVectors.toList.map(iv =>
                Some(Float8VectorWrapper(iv))
              ) ++ outputVectors.map(_ => None),
              outputArguments = inputVectors.toList.map(_ => None) ++ outputVectors.map(v =>
                Some(Float8VectorWrapper(v))
              )
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
        logger.debug(s"[$uuid] received ${outputVectors.head.getValueCount} items.")

        val result =
          try {
            (0 until outputVectors.head.getValueCount).map { v_idx =>
              outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
                val doubleV = v.getValueAsDouble(v_idx)
                writer.write(c_idx, doubleV)
              }
              writer.getRow.copy()
            }
          } finally {
            outputVectors.foreach(_.close())
            allocatorOut.close()
          }

        logger.debug(s"[$uuid] completed transfer.")

        result

      }
      .coalesce(numPartitions = 1, shuffle = true)
      .mapPartitions { unsafeRows =>
        Iterator
          .continually {
            val unsafeRowsList = unsafeRows.toList
            val isAggregation = resultExpressions.exists(
              _.asInstanceOf[Alias].child.isInstanceOf[AggregateExpression]
            )

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
                  val count = unsafeRowsList.map(_.getDouble(idx + 1)).sum
                  val result = sum / count
                  writer.write(outIdx, result)
                case (a @ Alias(AggregateExpression(Sum(_), _, _, _, _), _), outIdx) =>
                  val idx = startingIndices(a)
                  val result = unsafeRowsList.map(_.getDouble(idx)).sum
                  writer.write(outIdx, result)
                case other => sys.error(s"Other not supported: ${other}")
              }
              Iterator(writer.getRow)
            } else unsafeRowsList.iterator
          }
          .take(1)
          .flatten
      }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (child.supportsColumnar) executeColumnWise() else executeRowWise()
  }
}
