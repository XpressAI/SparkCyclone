package com.nec.spark.agile

import java.io.ByteArrayInputStream

import scala.util.Try

trait BundleExecutor[T] {
  parent =>
  def executeBundle(bundle: Bundle): T

  def map[V](f: T => V): BundleExecutor[V] = (bundle: Bundle) => f(parent.executeBundle(bundle))
}

object BundleExecutor {

  def returningBigDecimal: BundleExecutor[BigDecimal] =
    lines.map(result =>
      Try(BigDecimal(result.last)).getOrElse(sys.error(s"Could not parse result: $result"))
    )

  def lines: BundleExecutor[List[String]] = bundle => executePython(bundle.asPythonScript)

  def executePython(data: Seq[String]): List[String] = {
    val inputStream = new ByteArrayInputStream(data.head.getBytes("UTF-8"))
    val arguments = data.tail.mkString(" ")
    import scala.sys.process._
    try {
      (Seq("ssh", "a6", "python3", "-", arguments) #< inputStream).lineStream_!.toList
    } finally inputStream.close()
  }

}
