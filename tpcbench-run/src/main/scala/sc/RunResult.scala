package sc

final case class RunResult(
  succeeded: Boolean,
  wallTime: Int,
  queryTime: Int,
  appUrl: String,
  traceResults: String,
  logOutput: String
) {}
object RunResult {
  val fieldNames: List[String] = {
    classOf[RunResult].getDeclaredFields.map(_.getName).toList
  }
}
