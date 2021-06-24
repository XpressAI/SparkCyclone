package com.nec.spark.agile.wscg

import com.nec.spark.agile.wscg.ArrowSummingCodegenPlan.UnsafeArrowSummingContainer
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.BlockingOperatorWithCodegen
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.DoubleType

object ArrowSummingCodegenPlan {

  /** Container to aggregate all the data coming in and place it in the right spots */
  /** Will probably be MUCH faster if we put somewhere other than Arrow. */
  /** We can also batch the inputs and process asynchronously based on memory limitations */
  /** There're many variations we can do here */
  final class UnsafeArrowSummingContainer(summer: ArrowSummer, vectorSchemaRoot: VectorSchemaRoot)
    extends UnsafeBatchProcessor {
    private val theVector = vectorSchemaRoot.getVector(0).asInstanceOf[Float8Vector]
    private var counter: Int = 0
    override def insertRow(unsafeRow: UnsafeRow): Unit = {
      val vl = unsafeRow.getDouble(0)

      /**
       * This is bound to be quite slow, allocating memory for each record;
       * TODO find a better strategy
       */
      theVector.setValueCount(counter + 1)
      theVector.setSafe(counter, vl)
      counter = counter + 1
    }
    override def execute(): Iterator[InternalRow] = {
      val result = summer.sum(theVector, 1)
      val writer = new UnsafeRowWriter(1)
      writer.reset()
      writer.write(0, result)
      Iterator(writer.getRow)
    }
  }
}

final case class ArrowSummingCodegenPlan(child: SparkPlan, summer: ArrowSummer)
  extends SparkPlan
  with BlockingOperatorWithCodegen
  with UnsafeExternalProcessorBase {

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "value", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = {
    sys.error("This should not be called if in WSCG")
  }
  /*
 require(
    child.isInstanceOf[CodegenSupport],
    s"Required to support Codegen, but ${child.getClass} does not support it."
  )
   */
  override type ContainerType = UnsafeArrowSummingContainer

  def createContainer(): UnsafeArrowSummingContainer = {
    val timeZoneId = conf.sessionLocalTimeZone
    val arrowSchema: Schema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator(
        s"writer for a summing plan",
        0,
        Long.MaxValue
      )
    val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    new UnsafeArrowSummingContainer(summer, root)
  }

  override def containerClass: Class[UnsafeArrowSummingContainer] =
    classOf[UnsafeArrowSummingContainer]
}
