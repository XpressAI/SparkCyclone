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
  .dependsOn(`ve-direct`)

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
  "org.scalatest" %% "scalatest" % "3.2.7" % "test,acc",
  "frovedis" %% "frovedis-client" % "0.1.0-SNAPSHOT" % "test,acc",
  "frovedis-client" %% "frovedis-client" % "0.1.0-SNAPSHOT" % "test,acc",
  "frovedis" %% "frovedis-client-test" % "0.1.0-SNAPSHOT" % "test,acc",
  "com.nec" % "aveo4j" % "0.0.1"
)

Test / parallelExecution := false

lazy val AcceptanceTest = config("acc") extend Test
inConfig(AcceptanceTest)(Defaults.testTasks)

/** Acceptance tests basically run against external/SSH/etc */
val AcceptanceTestTag = "com.nec.spark.AcceptanceTest"
val excludeAcceptanceTestOption = Tests.Argument("-l", AcceptanceTestTag)

Test / testOptions += excludeAcceptanceTestOption
AcceptanceTest / testOptions += Tests.Argument("-C", "com.nec.spark.agile.MarkdownReporter")
AcceptanceTest / testOptions += Tests.Argument("-o")

AcceptanceTest / testOptions := (AcceptanceTest / testOptions).value
  .filter(_ != `excludeAcceptanceTestOption`)

/** in SBT, run: AcceptanceTest / test; Test / test * */

Global / onChangedBuildSource := ReloadOnSourceChanges

addCommandAlias("check", ";scalafmtCheck;scalafmtSbtCheck;testQuick")
addCommandAlias("fmt", ";scalafmtSbt;scalafmtAll")

lazy val deploy = taskKey[Unit]("Deploy artifacts to a6")

deploy := {
  val logger = streams.value.log
  import scala.sys.process._

  /**
   * Because we use `assembly`, it runs unit tests as well. If you are iterating and want to just
   * assemble without uploading, use: `set assembly / test := {}`
   */
  logger.info("Preparing deployment: assembling.")
  val generatedFile = assembly.value
  logger.info(s"Assembled file: ${generatedFile}")
  Seq("ssh", "a6", "mkdir", "-p", "/opt/aurora4spark/", "/opt/aurora4spark/examples/") ! logger
  logger.info(s"Uploading JAR")
  Seq("scp", generatedFile.toString, "a6:/opt/aurora4spark/aurora4spark-sql-plugin.jar") ! logger
  logger.info(s"Uploaded JAR")

  Seq(
    "scp",
    "-r",
    (baseDirectory.value / ".." / ".." / "examples").getAbsolutePath,
    "a6:/opt/aurora4spark/"
  ) ! logger
}

ThisBuild / resolvers += "frovedis-repo" at file("frovedis-ivy").toURI.toASCIIString
ThisBuild / resolvers += "aveo4j-repo" at Paths.get("..", "..", "aveo4j-repo").toUri.toASCIIString

lazy val `ve-direct` = project
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.7" % "test,it",
      "org.bytedeco" % "javacpp" % "1.5.5",
      "net.java.dev.jna" % "jna-platform" % "5.8.0",
      "commons-io" % "commons-io" % "2.8.0" % "it"
    ),
    IntegrationTest / managedResources := {
      val resourceBase = (IntegrationTest / resourceManaged).value
      val assembled = assembly.value
      val tgt = resourceBase / assembled.name
      IO.copyFile(assembled, tgt)
      Seq(tgt)
    },
    assembly / assemblyMergeStrategy := {
      case v if v.contains("module-info.class")   => MergeStrategy.discard
      case v if v.contains("reflect-config.json") => MergeStrategy.discard
      case v if v.contains("jni-config.json")     => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
