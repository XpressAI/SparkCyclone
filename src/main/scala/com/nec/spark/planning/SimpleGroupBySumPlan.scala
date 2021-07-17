package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import com.nec.arrow.functions.GroupBySum
import com.nec.native.NativeEvaluator
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod.{JvmArrowBased, VEBased}
import org.apache.arrow.vector.{VectorSchemaRoot, Float8Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed

object SimpleGroupBySumPlan {
  sealed trait GroupByMethod extends Serializable
  object GroupByMethod {
    case object JvmArrowBased extends GroupByMethod
    case object VEBased extends GroupByMethod
  }
}
final case class SimpleGroupBySumPlan(
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator,
  groupMethod: GroupByMethod
) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(GroupBySum.GroupBySumSourceCode)

    child
      .execute()
      .coalesce(1, shuffle = true)
      .mapPartitions(iterator => {
        val timeZoneId = conf.sessionLocalTimeZone
        val allocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"writer for word count", 0, Long.MaxValue)
        val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val arrowWriter = ArrowWriter.create(root)
        iterator.foreach(row => arrowWriter.write(row))
        arrowWriter.finish()

        val groupingVec = root.getVector(0).asInstanceOf[Float8Vector]
        val valuesVec = root.getVector(1).asInstanceOf[Float8Vector]
        val outGroupsVector = new Float8Vector("groups", ArrowUtilsExposed.rootAllocator)
        val outValuesVector = new Float8Vector("values", ArrowUtilsExposed.rootAllocator)

        try {
          val resultsMap = groupMethod match {
            case JvmArrowBased =>
              GroupBySum.groupBySumJVM(groupingVec, valuesVec)
            case VEBased =>
              evaluator.callFunction(
                "group_by_sum",
                List(
                  Some(Float8VectorWrapper(groupingVec)),
                  Some(Float8VectorWrapper(valuesVec)),
                  None,
                  None
                ),
                List(
                  None,
                  None,
                  Some(Float8VectorWrapper(outValuesVector)),
                  Some(Float8VectorWrapper(outGroupsVector))
                )
              )
              (0 until outGroupsVector.getValueCount)
                .map(idx => (outGroupsVector.get(idx), outValuesVector.get(idx)))
                .toMap
          }

          resultsMap.zipWithIndex.map {
            case ((groupingId, sum), idx) => {
              val writer = new UnsafeRowWriter(2)
              writer.reset()
              writer.write(0, groupingId)
              writer.write(1, sum)
              writer.getRow
            }
          }.toIterator
        } finally {
          groupingVec.close()
          valuesVec.close()
        }
      })
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference("group", DoubleType, false)(),
    AttributeReference("value", DoubleType, false)()
  )
}
