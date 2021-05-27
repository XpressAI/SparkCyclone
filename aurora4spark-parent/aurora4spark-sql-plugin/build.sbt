import sbt.Def.spaceDelimited
import sbt.librarymanagement.Configuration
import sbt.librarymanagement.Configurations.Runtime

import java.nio.file.Paths

/**
 * For fast development purposes, similar to how Spark project does it. Maven's compilation cycles
 * are very slow
 */
val sparkVersion = "3.1.1"
ThisBuild / scalaVersion := "2.12.13"
val orcVversion = "1.5.8"
val slf4jVersion = "1.7.30"

lazy val root = project
  .in(file("."))
  .configs(AcceptanceTest)
  .configs(VectorEngine)
  .configs(CMake)

lazy val example = project
  .dependsOn(root)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"
    )
  )

libraryDependencies ++= Seq(
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.7" % "test,acc,cmake,ve",
  "frovedis" %% "frovedis-client" % "0.1.0-SNAPSHOT" % "test,acc",
  "frovedis" %% "frovedis-client-test" % "0.1.0-SNAPSHOT" % "test,acc",
  "com.nec" % "aveo4j" % "0.0.1",
  "org.bytedeco" % "javacpp" % "1.5.5",
  "net.java.dev.jna" % "jna-platform" % "5.8.0",
  "commons-io" % "commons-io" % "2.8.0" % "test",
  "org.apache.arrow" % "arrow-vector" % "2.0.0",
  "org.apache.arrow" % "arrow-memory-unsafe" % "4.0.0",
  "com.google.flatbuffers" % "flatbuffers-java" % "1.9.0"
)

Test / parallelExecution := false

inConfig(Test)(Defaults.testTasks)

lazy val AcceptanceTest = config("acc") extend Test
inConfig(AcceptanceTest)(Defaults.testTasks)
def accFilter(name: String): Boolean = name.startsWith("acc")

lazy val VectorEngine = config("ve") extend Test
inConfig(VectorEngine)(Defaults.testTasks)
def veFilter(name: String): Boolean = name.startsWith("ve")
VectorEngine / fork := true

lazy val CMake = config("cmake") extend Test
inConfig(CMake)(Defaults.testTasks)
def cmakeFilter(name: String): Boolean = name.startsWith("cmake")
CMake / fork := true

def otherFilter(name: String): Boolean = !accFilter(name) && !veFilter(name) && !cmakeFilter(name)

Test / testOptions := Seq(Tests.Filter(otherFilter))
AcceptanceTest / testOptions := Seq(Tests.Filter(accFilter))
VectorEngine / testOptions := Seq(Tests.Filter(veFilter))
CMake / testOptions := Seq(Tests.Filter(cmakeFilter))

AcceptanceTest / testOptions += Tests.Argument("-C", "com.nec.spark.agile.MarkdownReporter")
AcceptanceTest / testOptions += Tests.Argument("-o")

Global / onChangedBuildSource := ReloadOnSourceChanges

addCommandAlias("check", ";scalafmtCheck;scalafmtSbtCheck;testQuick")

addCommandAlias(
  "compile-all",
  "; Test / compile ; ve-direct / Test / compile ; ve-direct / It / compile"
)

addCommandAlias("fmt", ";scalafmtSbt;scalafmtAll")

assembly / assemblyMergeStrategy := {
  case v if v.contains("module-info.class")   => MergeStrategy.discard
  case v if v.contains("reflect-config.json") => MergeStrategy.discard
  case v if v.contains("jni-config.json")     => MergeStrategy.discard
  case v if v.contains("git.properties")      => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val deploy = inputKey[Unit]("Deploy artifacts to `deployTarget`")

lazy val deployExamples = inputKey[Unit]("Deploy artifacts to `deployTarget`")

deploy := {
  val args: Seq[String] = spaceDelimited("<arg>").parsed
  val targetBox = args.headOption.getOrElse(sys.error("Deploy target missing"))
  val logger = streams.value.log
  import scala.sys.process._

  /**
   * Because we use `assembly`, it runs unit tests as well. If you are iterating and want to just
   * assemble without uploading, use: `set assembly / test := {}`
   */
  logger.info("Preparing deployment: assembling.")
  val generatedFile = assembly.value
  logger.info(s"Assembled file: ${generatedFile}")
  logger.info(s"Uploading JAR to ${targetBox}")
  Seq("ssh", targetBox, "mkdir", "-p", "/opt/aurora4spark/") ! logger
  Seq(
    "scp",
    generatedFile.toString,
    s"${targetBox}:/opt/aurora4spark/aurora4spark-sql-plugin.jar"
  ) ! logger
  logger.info(s"Uploaded JAR")
}

deployExamples := {
  val args: Seq[String] = spaceDelimited("<arg>").parsed
  val targetBox = args.headOption.getOrElse(sys.error("Deploy target missing"))
  val logger = streams.value.log
  import scala.sys.process._

  logger.info(s"Preparing deployment of examples to ${targetBox}...")
  Seq("ssh", targetBox, "mkdir", "-p", "/opt/aurora4spark/", "/opt/aurora4spark/examples/") ! logger
  logger.info("Created dir.")
  Seq(
    "scp",
    "-r",
    (baseDirectory.value / ".." / ".." / "examples").getAbsolutePath,
    s"${targetBox}:/opt/aurora4spark/"
  ) ! logger
  logger.info("Uploaded examples.")
}

ThisBuild / resolvers += "frovedis-repo" at file("frovedis-ivy").toURI.toASCIIString
ThisBuild / resolvers += "aveo4j-repo" at Paths.get("..", "..", "aveo4j-repo").toUri.toASCIIString
