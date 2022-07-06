package io.sparkcyclone.util

import sourcecode._

final case class CallContext(file: File,
                             line: Line,
                             fullName: FullName) {
  override def toString: String = {
    s"${fullName.value} (${file.value}#${line.value})"
  }
}

object CallContextOps {
  implicit def newContext(implicit file: File,
                          line: Line,
                          fullName: FullName): CallContext = {
    CallContext(file, line, fullName)
  }
}
