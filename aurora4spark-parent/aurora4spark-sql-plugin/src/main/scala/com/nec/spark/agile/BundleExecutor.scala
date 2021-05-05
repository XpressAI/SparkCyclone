package com.nec.spark.agile

import java.io.ByteArrayInputStream
import scala.util.Try

trait BundleExecutor[T] {
  parent =>
  def executeBundle(bundle: Bundle): T

  def map[V](f: T => V): BundleExecutor[V] = (bundle: Bundle) => f(parent.executeBundle(bundle))
}

object BundleExecutor {

  def returnBigDecimal(bundleExecutor: BundleExecutor[List[String]]): BundleExecutor[BigDecimal] =
    bundleExecutor.map(result =>
      Try(BigDecimal(result.last)).getOrElse(sys.error(s"Could not parse result: $result"))
    )

  def returningBigDecimalRemote: BundleExecutor[BigDecimal] =
    returnBigDecimal(remoteLines)

  def returningBigDecimalLocal: BundleExecutor[BigDecimal] =
    returnBigDecimal(localLines)

  def remoteLines: BundleExecutor[List[String]] = bundle => executePython(bundle.asPythonScript)

  def localLines: BundleExecutor[List[String]] = bundle =>
    executePythonLocally(bundle.asPythonScript)

  def executePython(data: String): List[String] = {
    import scala.sys.process._
    val inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"))
    try {
      (Seq("ssh", "ed", "python3", "-") #< inputStream).lineStream_!.toList
    } finally inputStream.close()
  }

  def executePythonLocally(data: String): List[String] = {
    val inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"))

    import scala.sys.process._
    try {
      (Seq("python3", "-") #< inputStream).lineStream_!.toList
    } finally inputStream.close()
  }

}
