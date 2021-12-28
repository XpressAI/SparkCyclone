package com.nec.cmake

import com.nec.spark.planning.Tracer.Mapped

final case class Spanner(scalaTcpDebug: ScalaTcpDebug, mapped: Mapped) {
  def spanIterator[T](name: String)(
    f: => Iterator[T]
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T] =
    scalaTcpDebug.spanIterator(mapped, name)(f)
}
