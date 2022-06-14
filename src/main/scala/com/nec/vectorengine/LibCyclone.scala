package com.nec.vectorengine

import java.nio.file.{Path, Paths}

object LibCyclone {
  final val CppTargetPath = "/cycloneve"

  lazy val SoPath: Path = {
    Paths.get(getClass.getResource(s"${CppTargetPath}/${FileName}").toURI)
  }

  final val FileName = "libcyclone.so"
  final val HandleTransferFn = "handle_transfer"
  final val FreeFn = "cyclone_free"
  final val AllocFn = "cyclone_alloc"
}
