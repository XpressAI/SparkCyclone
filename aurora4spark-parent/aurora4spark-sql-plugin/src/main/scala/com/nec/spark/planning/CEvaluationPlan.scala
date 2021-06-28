package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import com.nec.spark.planning.CEvaluationPlan.NativeEvaluator
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed

object CEvaluationPlan {
  trait NativeEvaluator extends Serializable {
    def forCode(code: String): ArrowNativeInterfaceNumeric
  }
}
final case class CEvaluationPlan(
  lines: List[String],
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode {
  protected def inputAttributes: Seq[Attribute] = child.output
  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )
  override def outputPartitioning: Partitioning = SinglePartition
  override protected def doExecute(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(lines.mkString("\n", "\n", "\n"))
    child
      .execute()
      .coalesce(numPartitions = 1)
      .mapPartitions { rows =>
        Iterator {
          val timeZoneId = conf.sessionLocalTimeZone
          val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
            s"writer for word count",
            0,
            Long.MaxValue
          )
          val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          val arrowWriter = ArrowWriter.create(root)
          rows.foreach(row => arrowWriter.write(row))
          arrowWriter.finish()
          val vector = root.getVector(0).asInstanceOf[Float8Vector]
          arrowWriter.finish()

          val outputVector = new Float8Vector("count", allocator)
          outputVector.allocateNew(1)
          outputVector.setValueCount(1)
          evaluator.callFunction(
            name = "f",
            inputArguments = List(Some(Float8VectorWrapper(vector)), None),
            outputArguments = List(None, Some(outputVector), None)
          )

          (0 until outputVector.getValueCount).iterator.map { idx =>
            val writer = new UnsafeRowWriter(1)
            writer.reset()
            writer.write(0, outputVector.getValueAsDouble(idx))
            writer.getRow
          }
        }.take(1).flatten
      }
  }
}
