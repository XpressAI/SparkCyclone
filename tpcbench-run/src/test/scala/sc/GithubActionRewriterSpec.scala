package sc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.{Files, Paths}

object GithubActionRewriterSpec {}
final class GithubActionRewriterSpec extends AnyFreeSpec {

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
                         |        required: true
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
  "We can rewrite the YAML" in {
    val path = Paths.get(".github/workflows/benchmark-collector.yml")
    Files.write(
      path,
      setInputs(GitHubInput.DefaultList)(new String(Files.readAllBytes(path))).getBytes()
    )
  }
}
