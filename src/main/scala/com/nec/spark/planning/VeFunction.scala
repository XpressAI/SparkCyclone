package com.nec.spark.planning

import com.nec.native.NativeFunction
import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.core.{CVector, VeType}
import com.nec.spark.planning.LibLocation.LibLocation
import java.nio.file.Path

sealed trait VeFunctionStatus

object VeFunctionStatus {
  def fromCodeLines(lines: CodeLines): RawCode = {
    RawCode(lines.cCode)
  }

  final case class RawCode(code: String) extends VeFunctionStatus {
    override def toString: String = {
      super.toString.take(25)
    }
  }

  final case class SourceCode(function: NativeFunction) extends VeFunctionStatus {
    override def toString: String = {
      s"${getClass.getSimpleName} ${function.name} (${function.hashId})"
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
      case VeFunctionStatus.RawCode(code) =>
        sys.error(s"Raw source code was not compiled to library: ${code.take(10)} (${code.hashCode})... Does your plan extend ${classOf[PlanCallsVeFunction]}?")

      case VeFunctionStatus.SourceCode(function) =>
        sys.error(s"Native function was not compiled to library: ${(function.name, function.hashId)} Does your plan extend ${classOf[PlanCallsVeFunction]}?")

      case VeFunctionStatus.Compiled(libLocation) =>
        libLocation.resolve
    }
  }
}
