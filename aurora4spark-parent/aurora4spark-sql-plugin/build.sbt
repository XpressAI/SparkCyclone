/**
 * For fast development purposes, similar to how Spark project does it. Maven's compilation cycles
 * are very slow
 */
val sparkVersion = "3.1.1"
ThisBuild / scalaVersion := "2.12.13"
val orcVversion = "1.5.8"
val slf4jVersion = "1.7.30"
libraryDependencies ++= Seq(
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.7" % "test,acc",
  "frovedis" %% "frovedis-client" % "0.1.0-SNAPSHOT" % "test,acc",
  "frovedis" %% "frovedis-client-test" % "0.1.0-SNAPSHOT" % "test,acc"
)

Test / parallelExecution := false

lazy val AcceptanceTest = config("acc") extend Test
configs(AcceptanceTest)
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
  val generatedFile = (Compile / packageBin).value
  Seq("ssh", "a6", "mkdir", "-p", "/opt/aurora4spark/") ! logger
  Seq("scp", generatedFile.toString, "a6:/opt/aurora4spark/aurora4spark-sql-plugin.jar") ! logger
  Seq(
    "scp",
    (baseDirectory.value / ".." / ".." / "examples" / "example-avg.py").getAbsolutePath,
    "a6:/opt/aurora4spark/example-avg.py"
  ) ! logger
}

ThisBuild / resolvers += "frovedis-repo" at file("frovedis-ivy").toURI.toASCIIString
