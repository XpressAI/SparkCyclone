package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterface.Float8VectorWrapper
import com.nec.arrow.functions.GroupBySum
import com.nec.native.NativeEvaluator
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod.{JvmArrowBased, VEBased}
import org.apache.arrow.vector.{BitVectorHelper, Float8Vector, VectorSchemaRoot}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ArrowUtilsExposed

final case class SimpleGroupBySumTwoColumnsPlan(
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator,
  groupMethod: GroupByMethod
) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    val evaluator = nativeEvaluator.forCode(GroupBySum.GroupBySumSourceCode)

    child
      .execute()
      .coalesce(1)
      .mapPartitions(iterator => {
        val timeZoneId = conf.sessionLocalTimeZone
        val allocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"writer for word count", 0, Long.MaxValue)
        val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val arrowWriter = ArrowWriter.create(root)
        try iterator.foreach(row => arrowWriter.write(row))
        finally arrowWriter.finish()

        val groupingVec = root.getVector(0) match {
          case vec: Float8Vector => vec
          case other =>
            sys.error(s"For simple groupBy, only Float8Vector input is supported, got ${other}.")
        }
        val secondGroupingVec = root.getVector(1) match {
          case vec: Float8Vector => vec
          case other =>
            sys.error(s"For simple groupBy, only Float8Vector input is supported, got ${other}.")
        }
        val valuesVec = root.getVector(2) match {
          case vec: Float8Vector => vec
          case other =>
            sys.error(s"For simple groupBy, only Float8Vector input is supported, got ${other}.")
        }
        val outGroupsVector = new Float8Vector("groups", ArrowUtilsExposed.rootAllocator)
        val secondOutGroupsVector =
          new Float8Vector("secondGroups", ArrowUtilsExposed.rootAllocator)

        val outValuesVector = new Float8Vector("values", ArrowUtilsExposed.rootAllocator)

        try {
          val resultsMap = groupMethod match {
            case JvmArrowBased =>
              GroupBySum.groupBySumJVM(groupingVec, secondGroupingVec, valuesVec)
            case VEBased =>
              import Float8VectorWrapper._
              evaluator.callFunction(
                "group_by_sum2",
                List(
                  Some(Float8VectorWrapper(groupingVec)),
                  Some(Float8VectorWrapper(secondGroupingVec)),
                  Some(Float8VectorWrapper(valuesVec)),
                  None,
                  None,
                  None
                ),
                List(
                  None,
                  None,
                  None,
                  Some(Float8VectorWrapper(outValuesVector)),
                  Some(Float8VectorWrapper(outGroupsVector)),
                  Some(Float8VectorWrapper(secondOutGroupsVector))
                )
              )
              val firstGroupColumn = (0 until outGroupsVector.getValueCount)
                .map {
                  case idx if (BitVectorHelper.get(outGroupsVector.getValidityBuffer, idx)) == 1 =>
                    Some(outGroupsVector.get(idx))
                  case _ => None
                }

              val secondGroupColumn = (0 until secondOutGroupsVector.getValueCount)
                .map {
                  case idx
                      if (BitVectorHelper.get(secondOutGroupsVector.getValidityBuffer, idx)) == 1 =>
                    Some(secondOutGroupsVector.get(idx))
                  case _ => None
                }

              val valuesColumn = (0 until valuesVec.getValueCount)
                .map {
                  case idx if (BitVectorHelper.get(outValuesVector.getValidityBuffer, idx)) == 1 =>
                    Some(outValuesVector.get(idx))
                  case _ => None
                }

              firstGroupColumn
                .zip(secondGroupColumn)
                .zip(valuesColumn)
                .map(kv => (kv._1._1, kv._1._2, kv._2))
          }

          resultsMap.zipWithIndex.map {
            case ((groupingId, secondGroupingId, sum), idx) => {
              val writer = new UnsafeRowWriter(3)
              writer.reset()
              if (groupingId.isDefined) writer.write(0, groupingId.get) else writer.setNullAt(0)
              if (secondGroupingId.isDefined) writer.write(1, secondGroupingId.get)
              else writer.setNullAt(1)
              if (sum.isDefined) writer.write(2, sum.get) else writer.setNullAt(2)
              writer.getRow
            }
          }.toIterator
        } finally {
          groupingVec.close()
          secondGroupingVec.close()
          valuesVec.close()
        }
      })
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference("group", DoubleType, false)(),
    AttributeReference("secondGroup", DoubleType, false)(),
    AttributeReference("value", DoubleType, false)()
  )
}
