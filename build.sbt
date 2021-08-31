import sbt.Def.spaceDelimited

import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Paths

val CMake = config("cmake") extend Test
val VectorEngine = config("ve") extend Test
/**
 * For fast development purposes, similar to how Spark project does it. Maven's compilation cycles
 * are very slow
 */
ThisBuild / scalaVersion := "2.12.10"
val orcVversion = "1.5.8"
val slf4jVersion = "1.7.30"

lazy val root = Project(id = "aurora4spark-sql-plugin", base = file("."))
  .configs(AcceptanceTest)
  .configs(VectorEngine)
  .configs(CMake)

/**
 * Run with:
 *
 * fun-bench / Jmh / run -h
 */
lazy val `fun-bench` = project
  .enablePlugins(JmhPlugin)
  .dependsOn(root % "compile->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "co.fs2" %% "fs2-io" % "3.0.6",
      "io.circe" %% "circe-literal" % "0.14.1",
      "io.circe" %% "circe-generic" % "0.14.1",
      "io.circe" %% "circe-parser" % "0.14.1"
    ),
    name := "funbench",
    Jmh / run := (Jmh / run).dependsOn((Test / test)).evaluated,
    Jmh / run / javaOptions += "-Djmh.separateClasspathJAR=true",
    Jmh / run / fork := true,
    Test / test :=
      Def.taskDyn {
        val basicTests = Def
          .sequential(
            (ThisProject / Test / testQuick).toTask(""),
            (root / Test / testQuick).toTask(""),
            (root / CMake / testQuick).toTask("")
          )
        val testsWithVe = Def.sequential(basicTests, (root / VectorEngine / testQuick).toTask(""))
        val doSkipTests = (Test / skip).value
        val isOnVe = sys.env.contains("NLC_LIB_I64")
        if (doSkipTests) Def.task(())
        else if (isOnVe) testsWithVe
        else basicTests
      }.value,
    Compile / sourceGenerators +=
      Def.taskDyn {
        val str = streams.value
        val smDir = (Compile / sourceManaged).value
        val tgt = smDir / "DynamicBenchmark.scala"
        if (!smDir.exists()) Files.createDirectories(smDir.toPath)

        Def.sequential(
          Def.task {
            clean
          },
          Def.task {
            (root / Test / runMain).toTask(s" com.nec.spark.GenerateBenchmarksApp ${tgt}").value
            Seq(tgt)
          }
        )
      }
  )

crossScalaVersions := Seq("2.12.10", "2.11.12")

val sparkVersion = SettingKey[String]("sparkVersion")

ThisBuild / sparkVersion := {
  val scalaV = scalaVersion.value
  if (scalaV.startsWith("2.12")) "3.1.1" else "2.3.2"
}

libraryDependencies ++= Seq(
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test,acc,cmake,ve",
  "com.eed3si9n.expecty" %% "expecty" % "0.15.4" % "test,acc,cmake,ve",
  "com.nec" % "aveo4j" % "0.0.1",
  "org.bytedeco" % "javacpp" % "1.5.5",
  "net.java.dev.jna" % "jna-platform" % "5.8.0",
  "commons-io" % "commons-io" % "2.8.0" % "test",
  "com.h2database" % "h2" % "1.4.200" % "test,ve",
  "org.reflections" % "reflections" % "0.9.12",
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test,ve,cmake",
  "commons-io" % "commons-io" % "2.10.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.scala-sbt.ipcsocket" % "ipcsocket" % "1.4.0",
  /** Log with logback in our scopes but not in production runs */
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % "test,acc,cmake,ve",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test,acc,cmake,ve",
  "co.fs2" %% "fs2-io" % "3.0.6" % "test,acc,cmake,ve"
).map(_.excludeAll(ExclusionRule("*", "log4j"), ExclusionRule("*", "slf4j-log4j12")))

libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n > 11 =>
      List("com.nvidia" %% "rapids-4-spark" % "0.5.0" % "test,ve")
    case _ => Nil
  }
}

Test / unmanagedJars ++= sys.env
  .get("CUDF_PATH")
  .map(path => new File((path)))
  .map(file => Seq(file))
  .getOrElse(Seq())
  .classpath

/** Because of Spark */
Test / parallelExecution := false
inConfig(Test)(Defaults.testTasks)

/** Vector Engine specific configuration */
VectorEngine / parallelExecution := false
inConfig(VectorEngine)(Defaults.testTasks)
def veFilter(name: String): Boolean = name.startsWith("com.nec.ve")
VectorEngine / fork := true
VectorEngine / run / fork := true

/** This generates a file 'java.hprof.txt' in the project root for very simple profiling. * */
VectorEngine / run / javaOptions ++= {
  // The feature was removed in JDK9, however for Spark we must support JDK8
  if (ManagementFactory.getRuntimeMXBean.getVmVersion.startsWith("1.8"))
    List("-agentlib:hprof=cpu=samples")
  else Nil
}
VectorEngine / sourceDirectory := baseDirectory.value / "src" / "test"
VectorEngine / testOptions := Seq(Tests.Filter(veFilter))

/** CMake specific configuration */
inConfig(CMake)(Defaults.testTasks)
def cmakeFilter(name: String): Boolean = name.startsWith("com.nec.cmake")
CMake / fork := true
CMake / testOptions := Seq(Tests.Filter(cmakeFilter))

Global / cancelable := true

def otherFilter(name: String): Boolean = !accFilter(name) && !veFilter(name) && !cmakeFilter(name)
Test / testOptions := Seq(Tests.Filter(otherFilter))

/** Acceptance Testing configuration */
AcceptanceTest / parallelExecution := false
AcceptanceTest / testOptions ++= {
  if ((AcceptanceTest / debugTestPlans).value) Seq(debugTestPlansArgument) else Seq.empty
}
lazy val AcceptanceTest = config("acc") extend Test
inConfig(AcceptanceTest)(Defaults.testTasks)
def accFilter(name: String): Boolean = name.startsWith("com.nec.acceptance")
AcceptanceTest / testOptions := Seq(Tests.Filter(accFilter))
AcceptanceTest / testOptions += Tests.Argument("-C", "com.nec.acceptance.MarkdownReporter")
AcceptanceTest / testOptions += Tests.Argument("-o")

Global / onChangedBuildSource := ReloadOnSourceChanges

addCommandAlias("check", ";scalafmtCheck;scalafmtSbtCheck;testQuick")

addCommandAlias(
  "compile-all",
  "; Test / compile ; ve-direct / Test / compile ; ve-direct / It / compile"
)

addCommandAlias(
  "testQuick-all",
  "; Test / testQuick ; CMake / testQuick ; AcceptanceTest / testQuick ; VectorEngine / testQuick"
)

addCommandAlias(
  "test-all",
  "; Test / test ; CMake / test ; AcceptanceTest / test ; VectorEngine / test"
)

addCommandAlias("fmt", ";scalafmtSbt;scalafmtAll")

assembly / assemblyMergeStrategy := {
  case v if v.contains("module-info.class") => MergeStrategy.discard
  case v if v.contains("UnusedStub") => MergeStrategy.first
  case v if v.contains("aopalliance") => MergeStrategy.first
  case v if v.contains("inject") => MergeStrategy.first
  case v if v.contains("reflect-config.json") => MergeStrategy.discard
  case v if v.contains("jni-config.json") => MergeStrategy.discard
  case v if v.contains("git.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val debugTestPlans = settingKey[Boolean]("Whether to output Spark plans during testing")

debugTestPlans := false

val debugTestPlansArgument = Tests.Argument(TestFrameworks.ScalaTest, "-Ddebug.spark.plans=true")

Test / testOptions ++= {
  if ((Test / debugTestPlans).value) Seq(debugTestPlansArgument) else Seq.empty
}

lazy val deploy = inputKey[Unit]("Deploy artifacts to `deployTarget`")

lazy val deployExamples = inputKey[Unit]("Deploy artifacts to `deployTarget`")

addCommandAlias(
  "deploy-all",
  "; deploy a5 ; deployExamples a5; set assembly / test := {}; deploy a6; deployExamples a6"
)

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

  if (targetBox == "local") {
    logger.info(s"Copying JAR locally to /opt/aurora4spark/aurora4spark-sql-plugin.jar:")
    Seq("cp", generatedFile.toString, "/opt/aurora4spark/aurora4spark-sql-plugin.jar") ! logger
    logger.info(s"Copied.")
  } else {
    logger.info(s"Uploading JAR to ${targetBox}")
    Seq("ssh", targetBox, "mkdir", "-p", "/opt/aurora4spark/") ! logger
    Seq(
      "scp",
      generatedFile.toString,
      s"${targetBox}:/opt/aurora4spark/aurora4spark-sql-plugin.jar"
    ) ! logger
    logger.info(s"Uploaded JAR")
  }
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
    (baseDirectory.value / "examples").getAbsolutePath,
    s"${targetBox}:/opt/aurora4spark/"
  ) ! logger
  Seq(
    "scp",
    (baseDirectory.value / "README.md").getAbsolutePath,
    s"${targetBox}:/opt/aurora4spark/"
  ) ! logger
  logger.info("Uploaded examples.")
}

ThisBuild / resolvers += "frovedis-repo" at file("frovedis-ivy").toURI.toASCIIString
ThisBuild / resolvers += "aveo4j-repo" at Paths.get("aveo4j-repo").toUri.toASCIIString

Test / testOptions += Tests.Argument("-oD")

val bench = inputKey[Unit]("Runs JMH benchmarks in fun-bench")
bench := (`fun-bench` / Jmh / run).evaluated

addCommandAlias("skipBenchTests", "; set `fun-bench` / Test / skip := true")
addCommandAlias("unskipBenchTests", "; set `fun-bench` / Test / skip := false")

val debugRemotePort = SettingKey[Option[Int]]("debugRemotePort")

ThisBuild / debugRemotePort := None

// set debugRemotePort := Some(5005)
// this will work in all the scopes because CMake and VectorEngine inherit from Test
// but specifying directly on CMake and VectorEngine will not work.
// if you want this setting to be persisted across SBT runs, update your ~/.sbt/1.0/local.sbt to include this specific setting
// in IntelliJ, this is the "Listen to remote JVM" option; also select 'Auto Restart'
Test / javaOptions ++= {
  debugRemotePort.value match {
    case None => Seq.empty
    case Some(port) => Seq("-Xdebug", s"-agentlib:jdwp=transport=dt_socket,server=n,address=127.0.0.1:${port},suspend=y,onuncaught=n")
  }
}

ThisBuild / scalacOptions ++= Seq("-Xfatal-warnings", "-feature", "-deprecation")
