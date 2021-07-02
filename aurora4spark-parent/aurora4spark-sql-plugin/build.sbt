import complete.DefaultParsers._

import scala.sys.process._
import sbt._
import sbt.Keys._
def intellijImportOnly212 = ideSkipProject := (scalaVersion.value != scala212)

import java.lang.management.ManagementFactory
import java.nio.file.Paths

val orcVversion = "1.5.8"
val slf4jVersion = "1.7.30"

lazy val scala212 = "2.12.14"
lazy val scala211 = "2.11.12"

val spark3Version = "3.1.1"
val spark2Version = "2.3.2"

lazy val sparkVersion = SettingKey[String]("sparkVersion")

ThisBuild / ideSkipProject := (scalaVersion.value != scala212)

val selectScalaVersions = Seq(scala211, scala212)
lazy val plugin = (projectMatrix in file("."))
  .settings(name := "aurora4spark-sql-plugin")
  .jvmPlatform(scalaVersions = selectScalaVersions)
  .settings(
    intellijImportOnly212,
    sparkVersion := {
      val sv = scalaVersion.value
        .contains(scala212)

      if (sv) spark3Version else spark2Version
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test,ve" classifier ("tests"),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test,ve" classifier ("tests"),
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test,ve" classifier ("tests"),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value,
      "com.nec" % "aveo4j" % "0.0.1",
      "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
      "org.scalatest" %% "scalatest" % "3.2.9" % "test,acc,cmake,ve",
      "com.eed3si9n.expecty" %% "expecty" % "0.15.4" % "test,acc,cmake,ve",
      "org.bytedeco" % "javacpp" % "1.5.5",
      "net.java.dev.jna" % "jna-platform" % "5.8.0",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "com.h2database" % "h2" % "1.4.200" % "test,ve",
      "org.reflections" % "reflections" % "0.9.12",
      "commons-io" % "commons-io" % "2.10.0"
    ),
    VectorEngine / sourceDirectory := baseDirectory.value / "src" / "test",
    /** Because of VE */
    VectorEngine / parallelExecution := false,
    /** Because of Spark */
    AcceptanceTest / parallelExecution := false,
    /** Because of Spark */
    Test / parallelExecution := false,
    CMake / fork := true,
    Test / testOptions := Seq(Tests.Filter(otherFilter)),
    AcceptanceTest / testOptions := Seq(Tests.Filter(accFilter)),
    VectorEngine / testOptions := Seq(Tests.Filter(veFilter)),
    CMake / testOptions := Seq(Tests.Filter(cmakeFilter)),
    AcceptanceTest / testOptions += Tests.Argument("-C", "com.nec.acceptance.MarkdownReporter"),
    AcceptanceTest / testOptions += Tests.Argument("-o"),
    VectorEngine / fork := true,
    VectorEngine / run / fork := true,
    /** This generates a file 'java.hprof.txt' in the project root for very simple profiling. * */
    VectorEngine / run / javaOptions ++= {
      // The feature was removed in JDK9, however for Spark we must support JDK8
      if (ManagementFactory.getRuntimeMXBean.getVmVersion.startsWith("1.8"))
        List("-agentlib:hprof=cpu=samples")
      else Nil
    },
    Test / testOptions ++= {
      if ((Test / debugTestPlans).value) Seq(debugTestPlansArgument) else Seq.empty
    },
    AcceptanceTest / testOptions ++= {
      if ((AcceptanceTest / debugTestPlans).value) Seq(debugTestPlansArgument) else Seq.empty
    },
    inConfig(CMake)(Defaults.testTasks),
    inConfig(Test)(Defaults.testTasks),
    inConfig(AcceptanceTest)(Defaults.testTasks),
    inConfig(VectorEngine)(Defaults.testSettings)
  )
  .configs(AcceptanceTest)
  .configs(VectorEngine)
  .configs(CMake)

/**
 * Run with:
 *
 * fun-bench / Jmh / run -h
 */
//lazy val `fun-bench` = projectMatrix
//  .in(file("fun-bench"))
//  .enablePlugins(JmhPlugin)
//  .dependsOn(plugin % "compile->test")
//  .settings(Jmh / run / javaOptions += "-Djmh.separateClasspathJAR=true")
//  .jvmPlatform(scalaVersions = selectScalaVersions)
//  .settings(Compile / sourceGenerators += Def.taskDyn {
//    val smDir = (Compile / sourceManaged).value
//    if (!smDir.exists()) Files.createDirectories(smDir.toPath)
//    val tgt = smDir / "KeyBenchmark.scala"
//
//    Def.taskDyn {
//      // run any outstanding unit tests, as if they are broken we are not the wisest to begin benchmarking!
//      (spark31Root / Test / testQuick).toTask("").value
//      Def.taskDyn {
//        val genTask =
//          (spark31Root / Test / runMain).toTask(s" com.nec.spark.GenerateBenchmarksApp ${tgt}")
//
//        Def.task {
//          genTask.value
//          Seq(tgt)
//        }
//      }
//    }
//  })

Test / unmanagedJars ++= sys.env
  .get("CUDF_PATH")
  .map(path => new File((path)))
  .map(file => Seq(file))
  .getOrElse(Seq())
  .classpath

lazy val AcceptanceTest = config("acc") extend Test

def accFilter(name: String): Boolean = name.startsWith("com.nec.acceptance")

lazy val VectorEngine = config("ve") extend Test
def veFilter(name: String): Boolean = name.startsWith("com.nec.ve")

Global / cancelable := true

lazy val CMake = config("cmake") extend Test

def cmakeFilter(name: String): Boolean = name.startsWith("com.nec.cmake")

def otherFilter(name: String): Boolean = !accFilter(name) && !veFilter(name) && !cmakeFilter(name)

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
  case v if v.contains("module-info.class")   => MergeStrategy.discard
  case v if v.contains("UnusedStub")          => MergeStrategy.first
  case v if v.contains("aopalliance")         => MergeStrategy.first
  case v if v.contains("/inject")             => MergeStrategy.first
  case v if v.contains("reflect-config.json") => MergeStrategy.discard
  case v if v.contains("jni-config.json")     => MergeStrategy.discard
  case v if v.contains("git.properties")      => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val debugTestPlans = settingKey[Boolean]("Whether to output Spark plans during testing")

ThisBuild / debugTestPlans := false

val debugTestPlansArgument = Tests.Argument(TestFrameworks.ScalaTest, "-Ddebug.spark.plans=true")

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

  logger.info(s"Preparing deployment of examples to ${targetBox}...")
  Seq("ssh", targetBox, "mkdir", "-p", "/opt/aurora4spark/", "/opt/aurora4spark/examples/") ! logger
  logger.info("Created dir.")
  Seq(
    "scp",
    "-r",
    (baseDirectory.value / ".." / ".." / "examples").getAbsolutePath,
    s"${targetBox}:/opt/aurora4spark/"
  ) ! logger
  Seq(
    "scp",
    (baseDirectory.value / ".." / ".." / "README.md").getAbsolutePath,
    s"${targetBox}:/opt/aurora4spark/"
  ) ! logger
  logger.info("Uploaded examples.")
}

ThisBuild / resolvers += "frovedis-repo" at file("frovedis-ivy").toURI.toASCIIString
ThisBuild / resolvers += "aveo4j-repo" at Paths.get("..", "..", "aveo4j-repo").toUri.toASCIIString

Test / testOptions += Tests.Argument("-oD")
