package com.nec.spark.planning

import com.nec.native.NativeFunction
import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.core.{CVector, VeType}
import com.nec.spark.planning.LibLocation.LibLocation
import java.nio.file.Path

sealed trait VeFunctionStatus

object VeFunctionStatus {
  def fromCodeLines(lines: CodeLines): SourceCode = {
    SourceCode(lines.cCode)
  }

  final case class SourceCode(code: String) extends VeFunctionStatus {
    override def toString: String = {
      super.toString.take(25)
    }
  }

  final case class NativeFunctions(functions: Seq[NativeFunction]) extends VeFunctionStatus {
    override def toString: String = {
      s"${getClass.getSimpleName} ${functions.map(_.name)}"
    }
  }

  final case class Compiled(libLocation: LibLocation) extends VeFunctionStatus
}

final case class VeFunction(status: VeFunctionStatus,
                            name: String,
                            outputs: Seq[CVector]) {
  def results: Seq[VeType] = {
    outputs.map(_.veType)
  }

  def isCompiled: Boolean = status match {
    case VeFunctionStatus.Compiled(_) =>
      true
    case _ =>
      false
  }

  def libraryPath: Path = {
    status match {
      case VeFunctionStatus.SourceCode(code) =>
        sys.error(s"Raw source code was not compiled to library: ${code.take(10)} (${code.hashCode})... Does your plan extend ${classOf[PlanCallsVeFunction]}?")

      case VeFunctionStatus.NativeFunctions(functions) =>
        sys.error(s"Native functions were not compiled to library: ${functions.map(x => (x.name, x.hashId))} Does your plan extend ${classOf[PlanCallsVeFunction]}?")

      case VeFunctionStatus.Compiled(libLocation) =>
        libLocation.resolve
    }
  }
}
