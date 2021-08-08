package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed

case class SimpleSortPlan( fName: String,
                      resultExpressions: Seq[NamedExpression],
                      lines: CodeLines,
                      child: SparkPlan,
                      inputReferences: Set[String],
                      nativeEvaluator: NativeEvaluator
) extends SparkPlan with UnaryExecNode {



  override protected def doExecute(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(lines.lines.mkString("\n", "\n", "\n"))
    child
      .execute()
      .coalesce(1)
      .mapPartitions { rows =>
        Iterator.continually {
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

            val inputVectors = output.zipWithIndex.map { case (attr, idx) =>
              root.getVector(idx)
            }
            arrowWriter.finish()

            val outputVectors = resultExpressions
              .zipWithIndex
              .map { case (ne, idx) =>
                val outputVector = new Float8Vector(s"out_${idx}", allocator)
                outputVector
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
                if (v_idx < v.getValueCount()) {
                  val doubleV = v.getValueAsDouble(v_idx)
                  writer.write(c_idx, doubleV)
                }
              }
              writer.getRow
            }

          }
          .take(1)
          .flatten
      }
  }

  override def output: Seq[Attribute] = child.output

}