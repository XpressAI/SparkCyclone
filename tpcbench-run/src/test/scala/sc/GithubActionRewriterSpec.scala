package sc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.circe.Decoder
import org.scalatest.freespec.AnyFreeSpec
import sc.GithubActionRewriterSpec.{makeGeneric, setInputs, yamlAs}

import java.nio.file.{Files, Paths}

object GithubActionRewriterSpec {
  def setInputs(inputs: Map[String, GitHubInput])(yaml: String): String = {

    /** SnakeYaml parses key 'on' as 'true' */
    val yf = new YAMLFactory()
    val om = new ObjectMapper(yf)
    val t = om.readTree(yaml)

    val inputsNode = t
      .get("on")
      .asInstanceOf[ObjectNode]
      .get("workflow_dispatch")
      .asInstanceOf[ObjectNode]
      .get("inputs")
      .asInstanceOf[ObjectNode]

    inputsNode.removeAll()
    inputs.foreach { case (key, v) =>
      v.createNode(key, inputsNode)
    }

    val e = t.get("env").asInstanceOf[ObjectNode]
    e.removeAll()

    inputs.keysIterator.foreach(k => e.put(s"INPUT_${k}", s"$${{ github.event.inputs.${k} }}"))

    om.writeValueAsString(t)
  }

  import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

  def convertYamlToJson(yaml: String): String = {
    val yamlReader = new ObjectMapper(new YAMLFactory)
    val obj = yamlReader.readValue(yaml, classOf[Any])
    val jsonWriter = new ObjectMapper()
    jsonWriter.writeValueAsString(obj)
  }

  def yamlAs[X: Decoder](yaml: String): X = {
    io.circe.parser.parse(convertYamlToJson(yaml)).fold(throw _, _.as[X]).fold(throw _, identity)
  }
  def makeGeneric(yaml: String): String = {

    /** SnakeYaml parses key 'on' as 'true' */
    val yf = new YAMLFactory()
    val om = new ObjectMapper(yf)
    val t = om.readTree(yaml)
    t.path("on")
      .path("workflow_dispatch")
      .path("inputs")
      .asInstanceOf[ObjectNode]
      .remove("query")
    val qa = t
      .path("jobs")
      .path("build")
      .asInstanceOf[ObjectNode]
      .putObject("strategy")
      .putObject("matrix")
      .putArray("query")

    t
      .asInstanceOf[ObjectNode]
      .put("name", "TPC-H Central All-Runner")

    t.path("env").asInstanceOf[ObjectNode].remove("INPUT_query")
    t.path("jobs")
      .path("build")
      .asInstanceOf[ObjectNode]
      .putObject("env")
      .put("INPUT_query", s"$${{ matrix.query }}")
    (1 to 22).map(x => qa.add(x))
    om.writeValueAsString(t)
  }
}

final class GithubActionRewriterSpec extends AnyFreeSpec {
  "We can set options in a YAML" in {
    val inputYaml = """name: bench
                      |on:
                      |  workflow_dispatch:
                      |    inputs:
                      |      test: something
                      |env:
                      |  x: a""".stripMargin
    val expectedYaml = """---
                         |name: "bench"
                         |"on":
                         |  workflow_dispatch:
                         |    inputs:
                         |      use-cyclone:
                         |        type: "boolean"
                         |        description: "Use Cyclone"
                         |        default: "true"
                         |env:
                         |  INPUT_use-cyclone: "${{ github.event.inputs.use-cyclone }}"
                         |""".stripMargin

    val result =
      setInputs(
        Map("use-cyclone" -> GitHubInput.OnOrOff(description = "Use Cyclone", default = true))
      )(inputYaml)
    assert(result == expectedYaml)
  }

  val pathColl = Paths.get(".github/workflows/benchmark-collector.yml")

  "We can rewrite the YAML" in {
    Files.write(
      pathColl,
      setInputs(GitHubInput.DefaultList)(new String(Files.readAllBytes(pathColl))).getBytes()
    )
  }

  val pathCollAll = pathColl.getParent.resolve("benchmark-collector-all.yml")

  def genV: String = makeGeneric(new String(Files.readAllBytes(pathColl)))

  "Make generic works" - {
    import io.circe.generic.auto._
    import io.circe._
    def y: Json = yamlAs[Json](genV)
    "Matrix is set up" in {
      val queryNos: List[Int] = (y \\ "jobs")
        .flatMap(_ \\ "build")
        .flatMap(_ \\ "strategy")
        .flatMap(_ \\ "matrix")
        .flatMap(_ \\ "query")
        .flatMap(_.asArray)
        .flatten
        .flatMap(_.asNumber)
        .flatMap(_.toInt)

      withClue(s"$y") {
        assert(queryNos.contains(21))
      }
    }
    "Query is removed from the choices" in {
      val r =
        (y \\ "on").flatMap(_ \\ "workflow_dispatch").flatMap(_ \\ "inputs").flatMap(_ \\ "query")
      assert(r.isEmpty, "Expecting query to not be there")
    }
    "INPUT_query is not set globally" in {
      val qv = y.asObject.flatMap(_.apply("env")).toList.flatMap(_ \\ "INPUT_query")
      withClue(s"$y") {
        assert(qv.isEmpty, "Expected there not to be INPUT_query at global level")
      }
    }
    "INPUT_query is set at job level" in {
      val qv = (y \\ "jobs")
        .flatMap(_ \\ "build")
        .flatMap(_ \\ "env")
        .flatMap(_ \\ "INPUT_query")
        .flatMap(_.asString)
      assert(qv == List(s"$${{ matrix.query }}"))
    }
  }
  "We can generate a YAML that will run for everything (generic)" in {
    Files.write(pathCollAll, genV.getBytes())
  }
}
