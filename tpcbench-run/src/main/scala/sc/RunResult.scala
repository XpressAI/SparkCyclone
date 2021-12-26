package sc

final case class RunResult(
  succeeded: Boolean,
  wallTime: Int,
  compileTime: String,
  queryTime: String,
  appUrl: String,
  traceResults: String,
  logOutput: String,
  containerList: String,
  metrics: String,
  finalPlan: Option[String]
) {}
object RunResult {
  val fieldNames: List[String] = {
    classOf[RunResult].getDeclaredFields.map(_.getName).toList
  }
}
