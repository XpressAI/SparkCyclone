//package io.sparkcyclone.spark.planning
//
//import scala.collection.JavaConverters.asScalaBufferConverter
//
//import org.apache.arrow.memory.BufferAllocator
//import org.apache.arrow.vector.VectorSchemaRoot
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.freespec.AnyFreeSpec
//import org.scalatest.matchers.should.Matchers
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.execution.arrow.ArrowWriter
//import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
//import org.apache.spark.sql.util.ArrowUtilsExposed
//import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
//
//class RowToArrowColumnarPlanSpec extends AnyFlatSpec with Matchers {
//
//  "RowToArrowColumnarPlan" should "correctly map row to arrow column vectors" in {
//    RowToArrowColumnarPlan.collectInputRows()
//
//  }
//}
