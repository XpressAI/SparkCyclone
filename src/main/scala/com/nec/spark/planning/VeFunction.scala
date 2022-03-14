package com.nec.spark.planning

import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeType}
import com.nec.spark.planning.LibLocation.LibLocation
import com.nec.spark.planning.VeFunction.VeFunctionStatus

object VeFunction {
  sealed trait VeFunctionStatus
  object VeFunctionStatus {
    def fromCodeLines(lines: CodeLines): SourceCode = SourceCode(lines.cCode)

    final case class SourceCode(sourceCode: String) extends VeFunctionStatus {
      override def toString: String = super.toString.take(25)
    }
    final case class Compiled(libLocation: LibLocation) extends VeFunctionStatus
  }
}

final case class VeFunction(
  veFunctionStatus: VeFunctionStatus,
  functionName: String,
  namedResults: List[CVector]
) {
  def results: List[VeType] = namedResults.map(_.veType)
  def isCompiled: Boolean = veFunctionStatus match {
    case VeFunctionStatus.SourceCode(_) => false
    case VeFunctionStatus.Compiled(_)   => true
  }
  def libraryPath: String = veFunctionStatus match {
    case VeFunctionStatus.SourceCode(path) =>
      sys.error(s"Library was not compiled; expected to build from ${path
        .take(10)} (${path.hashCode})... Does your plan extend ${classOf[PlanCallsVeFunction]}?")
    case VeFunctionStatus.Compiled(libLocation) => libLocation.resolveLocation().toString
  }
}
