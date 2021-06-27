package com.nec.spark.agile

import com.nec.spark.agile.ArrowSummingCodegenPlan.UnsafeArrowSummingContainer
import com.nec.spark.cgescape.UnsafeExternalProcessorBase
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.simplesum.ArrowUnsafeSummer
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.BlockingOperatorWithCodegen
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.util.ArrowUtilsExposed

object ArrowSummingCodegenPlan {}

final case class ArrowSummingCodegenPlan(child: SparkPlan, summer: ArrowSummer)
  extends SparkPlan
  with BlockingOperatorWithCodegen
  with UnsafeExternalProcessorBase {
  override def output: Seq[Attribute] = child.output
  override def children: Seq[SparkPlan] = Seq(child)
  override type ContainerType = ArrowUnsafeSummer
  def createContainer(): ArrowUnsafeSummer = {
    val timeZoneId = conf.sessionLocalTimeZone
    val arrowSchema: Schema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtilsExposed.rootAllocator.newChildAllocator(
        s"writer for a summing plan",
        0,
        Long.MaxValue
      )
    val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    new ArrowUnsafeSummer(summer, root)
  }

  override def containerClass: Class[ArrowUnsafeSummer] =
    classOf[ArrowUnsafeSummer]
}
