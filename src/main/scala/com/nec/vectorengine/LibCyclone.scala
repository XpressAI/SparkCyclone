package com.nec.vectorengine

import java.nio.file.{Path, Paths}

object LibCyclone {
  final val CppTargetPath = "/cycloneve"

  lazy val SoPath: Path = {
    Paths.get(getClass.getResource(s"${CppTargetPath}/libcyclone.so").toURI)
  }

  final val HandleTransferFn = "handle_transfer"
}
