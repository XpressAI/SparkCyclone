package com.nec.spark.planning

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.{CFunction, VeString}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, VarCharVector}
import org.apache.arrow.vector.util.Text

object Tracer {
  val TracerDefName = "TRACER"
  val TracerName = "tracer"
  val DefineTracer: CodeLines = CodeLines.from(s"#define ${TracerDefName} ${TracerName}")
  val TracerVector: CFunctionGeneration.CVector = VeString.makeCVector(TracerName)
  def includeInFunction(cFunctionNaked: CFunction): CFunction = {
    cFunctionNaked.copy(inputs = TracerVector :: cFunctionNaked.inputs)
  }

  def includeInInputs(tracer: VarCharVector, l: List[FieldVector]): List[FieldVector] = {
    tracer :: l
  }

  final case class Mapped(launched: Launched, mappingId: String) {
    import launched.launchId
    def uniqueId = s"$launchId-$mappingId"

    def createVector()(implicit bufferAllocator: BufferAllocator): VarCharVector = {
      val tracer = CFunctionGeneration.allocateFrom(TracerVector).asInstanceOf[VarCharVector]
      tracer.setValueCount(1)
      tracer.setSafe(0, new Text(s"$uniqueId"))
      tracer
    }
  }

  final case class Launched(launchId: String) {
    def map(mappingId: String): Mapped = Mapped(this, mappingId)
  }

  val TracerOutput: List[String] =
    List(
      s"std::string(${TracerDefName}->data, ${TracerDefName}->offsets[0], ${TracerDefName}->offsets[1] - ${TracerDefName}->offsets[0])"
    )

  def concatStr(parts: List[String]): String =
    parts.map(pt => s" << ${pt} ").mkString

}
