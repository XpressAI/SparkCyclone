package sc

import com.fasterxml.jackson.databind.node.ObjectNode

import scala.collection.immutable.SortedMap

sealed trait GitHubInput {
  def createNode(key: String, target: ObjectNode): Unit

  def description: String
}

object GitHubInput {

  val DefaultList: SortedMap[String, GitHubInput] = SortedMap(
    "query" -> Choice("Query to use", Some("1"), (1 to 22).map(_.toString).toList ++ List("all")),
    "use-cyclone" -> OnOrOff("Enable Spark Cyclone plugin?", default = true),
    "scale" -> Choice("Scale", Some("1"), List("1", "10", "20")),
    "serializer" -> Choice(
      description = "Serializer",
      default = Some("-"),
      options = List(
        "-",
        "io.sparkcyclone.spark.planning.VeCachedBatchSerializer",
        "io.sparkcyclone.cache.ArrowBasedCacheSerializer",
        "io.sparkcyclone.cache.InVectorEngineCacheSerializer"
      )
    ),
    "ve-log-debug" -> OnOrOff("Debug VE logs", default = false),
    "extra" -> Input("Extra command line arguments to add to Spark"),
    "pass-through-project" -> OnOrOff("Pass-through in projection", default = false),
    "fail-fast" -> OnOrOff("Fail Fast", default = true),
    "join-on-ve" -> OnOrOff("Join on VE", default = false),
  )

  final case class Choice(description: String, default: Option[String], options: List[String])
    extends GitHubInput {
    override def createNode(key: String, target: ObjectNode): Unit = {
      val m = target
        .putObject(key)
        .put("type", "choice")
        .put("description", description)

      val arr = m.putArray("options")
      options.foreach(o => arr.add(o))

      default.map(m.put("default", _)).getOrElse(m)
    }

  }

  final case class Input(description: String) extends GitHubInput {
    override def createNode(key: String, target: ObjectNode): Unit = target
      .putObject(key)
      .put("type", "input")
      .put("description", description)
  }
  final case class OnOrOff(description: String, default: Boolean) extends GitHubInput {
    override def createNode(key: String, target: ObjectNode): Unit =
      target
        .putObject(key)
        .put("type", "boolean")
        .put("description", description)
        .put("default", default.toString)
  }
}
