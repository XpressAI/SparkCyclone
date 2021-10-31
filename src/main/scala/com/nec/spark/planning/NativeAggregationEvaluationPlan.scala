/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.planning

import java.util.UUID

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.language.dynamics

import com.nec.arrow.AccessibleArrowColumnVector
import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.cmake.ScalaTcpDebug
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.agile.{CFunctionGeneration, SparkExpressionToCExpression}
import com.nec.spark.planning.NativeAggregationEvaluationPlan.EvaluationMode.{PrePartitioned, TwoStaged}
import com.nec.spark.planning.NativeAggregationEvaluationPlan.{EvaluationMode, writeVector}
import com.nec.spark.planning.Tracer.DefineTracer
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

//noinspection DuplicatedCode
final case class NativeAggregationEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  functionPrefix: String,
  evaluationMode: EvaluationMode,
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def supportsColumnar: Boolean = true

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

  def collectInputRows(inputRows: Iterator[InternalRow], target: List[FieldVector])(implicit
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val root = new VectorSchemaRoot(target.asJava)
    val arrowWriter = ArrowWriter.create(root)
    inputRows.foreach(row => arrowWriter.write(row))

    arrowWriter.finish()
    root
  }

  private def executeRowWise(twoStaged: TwoStaged): RDD[InternalRow] = {
    import twoStaged._
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

          (0 until partialOutputVectors.head.getValueCount).iterator.map { v_idx =>
            val writer = new UnsafeRowWriter(partialOutputVectors.size)
            writer.reset()
            partialOutputVectors.zipWithIndex.foreach { case (v, c_idx) =>
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
                  case varChar: VarCharVector =>
                    val isNull = BitVectorHelper.get(varChar.getValidityBuffer, v_idx) == 0
                    if (isNull) writer.setNullAt(c_idx)
                    else writer.write(c_idx, varChar.get(v_idx))
                }
              }
            }
            writer.getRow
          }
        } finally {
          inputVectors.foreach(_.close())
        }
      }
      .coalesce(1, shuffle = true)
      .mapPartitions { iteratorColBatch =>
        Iterator
          .continually {

            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for final collector", 0, Long.MaxValue)
            val partialInputVectors: List[FieldVector] =
              finalFunction.inputs.map(CFunctionGeneration.allocateFrom(_))

            collectInputRows(iteratorColBatch, partialInputVectors)

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
                CFunctionGeneration.allocateFrom(
                  SparkExpressionToCExpression
                    .sparkTypeToVeType(ne.dataType)
                    .makeCVector(s"out_${idx}")
                )(allocator)
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
                  if (v_idx < v.getValueCount) writeVector(v_idx, writer, v, c_idx)
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

  private def executeColumnWise(twoStaged: TwoStaged): RDD[ColumnarBatch] = {
    import twoStaged._
    val partialFunctionName = s"${functionPrefix}_partial"
    val finalFunctionName = s"${functionPrefix}_final"

    val evaluator = nativeEvaluator.forCode(
      List(
        partialFunction.toCodeLines(partialFunctionName),
        finalFunction
          .toCodeLinesNoHeader(finalFunctionName)
      ).reduce(_ ++ _).lines.mkString("\n", "\n", "\n")
    )

    logger.debug(s"Will execute columnar NewCEvaluationPlan for child ${child}; ${child.output}")

    child
      .executeColumnar()
      .mapPartitions { batches =>
        batches.map { batch =>

          implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)

          val inputVectors = child
            .output
            .indices
            .map(batch.column(_).asInstanceOf[AccessibleArrowColumnVector].getArrowValueVector)

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
            val vectors = partialOutputVectors
              .map(vector => new AccessibleArrowColumnVector(vector))
            new ColumnarBatch(vectors.toArray, vectors.head.getArrowValueVector.getValueCount)
          } finally {
            inputVectors.foreach(_.close())
          }
        }
      }
      .coalesce(1, shuffle = true)
      .mapPartitions { iteratorColBatch => iteratorColBatch.map{batch =>

            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for final collector", 0, Long.MaxValue)

            val partialInputVectors: List[ValueVector] =
              finalFunction
                .inputs
                .indices
                .map(idx => batch.column(idx).asInstanceOf[AccessibleArrowColumnVector].getArrowValueVector)
                .toList

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
                CFunctionGeneration.allocateFrom(
                  SparkExpressionToCExpression
                    .sparkTypeToVeType(ne.dataType)
                    .makeCVector(s"out_${idx}")
                )(allocator)
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

              val outputColumnVectors = outputVectors.map(vector => new AccessibleArrowColumnVector(vector))
              new ColumnarBatch(outputColumnVectors.toArray, cnt)
            } finally {
              partialInputVectors.foreach(_.close())
            }
          }
      }
  }

  private def executeOneGo(cFunctionNaked: CFunction): RDD[InternalRow] = {
    val cFunction = Tracer.includeInFunction(cFunctionNaked)
    val functionName = s"${functionPrefix}_full"

    val compilerConfig = VeCompilerConfig.fromSparkConf(sparkContext.getConf)
    val udpDebug = compilerConfig.maybeProfileTarget
      .map(pt => ScalaTcpDebug.TcpTarget(pt))
      .getOrElse(ScalaTcpDebug.NoOp)

    val launched = Tracer.Launched(
      s"${sparkContext.appName}|${sparkContext.applicationId}|${java.time.Instant.now().toString}"
    )

    val evaluator = udpDebug.span(launched.launchId, "prepare evaluator") {
      nativeEvaluator.forCode(
        List(DefineTracer, cFunction.toCodeLines(functionName))
          .reduce(_ ++ _)
          .lines
          .mkString("\n", "\n", "\n")
      )
    }

    logger.debug(
      s"[${launched.launchId}] Will execute NewCEvaluationPlan for child ${child}; ${child.output}"
    )

    child
      .execute()
      .mapPartitions { rows =>
        Iterator
          .continually {
            val executorId: String = SparkEnv.get.executorId
            val mapped = launched.map(s"${executorId}|${UUID.randomUUID().toString.take(4)}")
            import mapped._
            udpDebug.span(uniqueId, "evaluate a partition") {
              implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
                .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)
              val timeZoneId = conf.sessionLocalTimeZone
              val root = udpDebug.span(uniqueId, "collect input rows") {
                collectInputRows(rows, ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId))
              }
              val tracer = createVector()
              logger.debug(s"[$uniqueId] preparing execution")
              logger.info(s"Tracer ==> ${tracer}")
              val inputVectors =
                Tracer.includeInInputs(tracer, child.output.indices.map(root.getVector).toList)
              val outputVectors: List[FieldVector] =
                cFunction.outputs.map(CFunctionGeneration.allocateFrom(_))

              try {
                val outputArgs = inputVectors.map(_ => None) ++
                  outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
                val inputArgs = inputVectors.map(iv =>
                  Some(SupportedVectorWrapper.wrapInput(iv))
                ) ++ outputVectors.map(_ => None)

                val startTime = java.time.Instant.now()
                udpDebug.span(uniqueId, "evaluate") {
                  evaluator.callFunction(
                    name = functionName,
                    inputArguments = inputArgs,
                    outputArguments = outputArgs
                  )
                }
                val endTime = java.time.Instant.now()
                val timeTaken = java.time.Duration.between(startTime, endTime)

                val cnt = outputVectors.head.getValueCount
                logger.info(s"[$uniqueId] Got ${cnt} results back in ${timeTaken}")
                udpDebug.spanIterator(uniqueId, "emit rows") {
                  (0 until cnt).iterator.map { v_idx =>
                    val writer = new UnsafeRowWriter(outputVectors.size)
                    writer.reset()
                    outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
                      if (v_idx < v.getValueCount) writeVector(v_idx, writer, v, c_idx)
                    }
                    writer.getRow
                  }
                }
              } finally {
                inputVectors.foreach(_.close())
              }
            }
          }
          .take(1)
      }
      .flatMap(identity)
  }

  private def executeOneGoColumnar(cFunctionNaked: CFunction): RDD[ColumnarBatch] = {
    val cFunction = Tracer.includeInFunction(cFunctionNaked)
    val functionName = s"${functionPrefix}_full"

    val compilerConfig = VeCompilerConfig.fromSparkConf(sparkContext.getConf)
    val udpDebug = compilerConfig.maybeProfileTarget
      .map(pt => ScalaTcpDebug.TcpTarget(pt))
      .getOrElse(ScalaTcpDebug.NoOp)

    val launched = Tracer.Launched(
      s"${sparkContext.appName}|${sparkContext.applicationId}|${java.time.Instant.now().toString}"
    )

    val evaluator = udpDebug.span(launched.launchId, "prepare evaluator") {
      nativeEvaluator.forCode(
        List(DefineTracer, cFunction.toCodeLines(functionName))
          .reduce(_ ++ _)
          .lines
          .mkString("\n", "\n", "\n")
      )
    }

    logger.debug(
      s"[${launched.launchId}] Will execute NewCEvaluationPlan for child ${child}; ${child.output}"
    )

    child
      .executeColumnar()
      .mapPartitions { batches => batches.map { batch =>
            val executorId: String = SparkEnv.get.executorId
            val mapped = launched.map(s"${executorId}|${UUID.randomUUID().toString.take(4)}")
            import mapped._
            udpDebug.span(uniqueId, "evaluate a partition") {
              implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
                .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)

              val inputValueVectors = (0 until batch.numCols())
                .map(idx => batch.column(idx).asInstanceOf[AccessibleArrowColumnVector].getArrowValueVector)

              val tracer = createVector()
              logger.debug(s"[$uniqueId] preparing execution")
              logger.info(s"Tracer ==> ${tracer}")
              val inputVectors =
                Tracer.includeInInputs(tracer, inputValueVectors.toList)
              val outputVectors: List[FieldVector] =
                cFunction.outputs.map(CFunctionGeneration.allocateFrom(_))

              try {
                val outputArgs = inputVectors.map(_ => None) ++
                  outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
                val inputArgs = inputVectors.map(iv =>
                  Some(SupportedVectorWrapper.wrapInput(iv))
                ) ++ outputVectors.map(_ => None)

                val startTime = java.time.Instant.now()
                udpDebug.span(uniqueId, "evaluate") {
                  evaluator.callFunction(
                    name = functionName,
                    inputArguments = inputArgs,
                    outputArguments = outputArgs
                  )
                }
                val endTime = java.time.Instant.now()
                val timeTaken = java.time.Duration.between(startTime, endTime)

                val cnt = outputVectors.head.getValueCount
                logger.info(s"[$uniqueId] Got ${cnt} results back in ${timeTaken}")
                val arrowOutputVectors = outputVectors.map(vec => new AccessibleArrowColumnVector(vec))

                new ColumnarBatch(arrowOutputVectors.toArray, cnt)
              } finally {
                inputVectors.foreach(_.close())
              }
            }
          }
      }
  }

  override protected def doExecute(): RDD[InternalRow] = {

    evaluationMode match {
      case ts @ TwoStaged(_, _)      =>  executeRowWise(ts)

      case PrePartitioned(cFunction) => executeOneGo(cFunction)

    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    evaluationMode match {
      case ts @ TwoStaged(_, _)      => executeColumnWise(ts)

      case PrePartitioned(cFunction) => executeOneGoColumnar(cFunction)
    }
  }
}

object NativeAggregationEvaluationPlan {

  sealed trait EvaluationMode extends Serializable
  object EvaluationMode {
    final case class PrePartitioned(cFunction: CFunction) extends EvaluationMode
    final case class TwoStaged(partialFunction: CFunction, finalFunction: CFunction)
      extends EvaluationMode
  }

  def writeVector(v_idx: Int, writer: UnsafeRowWriter, v: FieldVector, c_idx: Int): Unit = {
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
