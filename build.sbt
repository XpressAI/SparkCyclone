import sbt.Def.spaceDelimited
import sbt.Keys.envVars

import scala.collection.mutable.{Buffer => MSeq, Map => MMap}
import scala.sys.process.{Process, ProcessLogger}
import scala.xml.Properties.isWin
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}

/**
 * *****************************************************************************
 * Global Settings
 * *****************************************************************************
 */

Global / cancelable := true
Global / onChangedBuildSource := ReloadOnSourceChanges

/*
  NOTE: Spark 2.12_3.3.0 has a dependency on 2.12.15.  Upgrades to newer minor
  versions must be done carefully, as it may cause issues since Spark uses some
  Scala library internals.
*/
lazy val defaultScalaVersion = "2.12.16"
ThisBuild / scalaVersion := defaultScalaVersion
ThisBuild / organization := "io.sparkcyclone"

val sparkVersion = SettingKey[String]("sparkVersion")
ThisBuild / sparkVersion := "3.3.0"

val debugRemotePort = SettingKey[Option[Int]]("debugRemotePort")
// set debugRemotePort := Some(5005)
ThisBuild / debugRemotePort := None

// Override the version of JavaCPP to be used
javaCppVersion := "1.5.7"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

lazy val slf4jVersion = "1.7.36"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.11" % "test,acc,ve",
  "com.eed3si9n.expecty" %% "expecty" % "0.15.4" % "test,acc,ve",
  "com.h2database" % "h2" % "2.1.214" % "test,ve",
  "com.lihaoyi" %% "scalatags" % "0.11.1" % "test,tpc",
  "com.lihaoyi" %% "sourcecode" % "0.3.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "com.vladsch.flexmark" % "flexmark-all" % "0.62.2" % "test,tpc",
  "commons-io" % "commons-io" % "2.11.0",
  // "commons-io" % "commons-io" % "2.10.0" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.bytedeco" % "veoffload" % "2.8.2-1.5.7-SNAPSHOT" classifier "linux-x86_64",
  "org.bytedeco" % "veoffload" % "2.8.2-1.5.7-SNAPSHOT",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided",
  "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test,acc,ve",
  "org.scalatestplus" %% "scalacheck-1-16" % "3.2.12.0" % "test,ve",
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
  // Log with logback in our scopes but not in production runs
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % "test,acc,ve",

).map(_.excludeAll(ExclusionRule("*", "log4j"), ExclusionRule("*", "slf4j-log4j12")))

lazy val VectorEngineTestTag = "io.sparkcyclone.annotations.VectorEngineTest"

/**
 * *****************************************************************************
 * Main Build Settings
 * *****************************************************************************
 */

lazy val root = Project(id = "spark-cyclone-sql-plugin", base = file("."))
  .configs(AcceptanceTest)
  .configs(VectorEngine)
  .configs(TPC)
  .settings(version := "1.0.5-SNAPSHOT")


/**
 * *****************************************************************************
 * Main Test Settings
 * *****************************************************************************
 */

inConfig(Test)(Defaults.testTasks)

// Spark-based tests need to be run sequentially
Test / parallelExecution := false
Test / unmanagedJars ++= sys.env
  .get("CUDF_PATH")
  .map(path => new File(path))
  .toList
  .classpath

Test / testOptions := {
  val options = if ((Test / debugToHtml).value) {
    Seq(
      Tests.Argument("-h", "target/test-html"),
      Tests.Argument("-Dmarkup=true")
    )
  } else {
    Seq.empty
  }

  options ++ Seq(
    Tests.Filter { name: String => !accFilter(name) && !tpcFilter(name)  },
    Tests.Argument("-oD"),
    Tests.Argument("-l", VectorEngineTestTag)
  )
}

Test / javaOptions ++= {
  /*
    Specifying the option here will work in all scopes that inherit the Test
    scope, but specifying directly in the individual scopes will not work.

    If you want this setting to be persisted across SBT runs, update
    `~/.sbt/1.0/local.sbt` to include this specific setting.  In IntelliJ, this
    corresponds to the `Listen to remote JVM` option; also select `Auto Restart`.
  */
  debugRemotePort.value match {
    case None => Seq.empty
    case Some(port) =>
      Seq(
        "-Xdebug",
        s"-agentlib:jdwp=transport=dt_socket,server=n,address=127.0.0.1:${port},suspend=y,onuncaught=n"
      )
  }
}


/**
 * *****************************************************************************
 * Vector Engine Test Settings
 * *****************************************************************************
 */

lazy val VectorEngine = config("ve") extend Test
inConfig(VectorEngine)(Defaults.testTasks)

VectorEngine / sourceDirectory := baseDirectory.value / "src" / "test"
VectorEngine / testOptions := Seq(Tests.Argument("-n", VectorEngineTestTag))
VectorEngine / parallelExecution := false
VectorEngine / fork := true
VectorEngine / run / fork := true

/**
 * Make each test suite run independently:
 * https://stackoverflow.com/questions/61072140/forking-each-scalatest-suite-with-sbt
 */
VectorEngine / testGrouping := (VectorEngine / definedTests).value.map { suite =>
  import sbt.Tests._
  Group(suite.name, Seq(suite), SubProcess(ForkOptions()))
}

// Generate `java.hprof.txt` in the project root for very simple profiling
VectorEngine / run / javaOptions ++= {
  // The feature was removed in JDK9, however for Spark we must support JDK8
  if (ManagementFactory.getRuntimeMXBean.getVmVersion.startsWith("1.8")) {
    List("-agentlib:hprof=cpu=samples")
  } else {
    Nil
  }
}


/**
 * *****************************************************************************
 * TPCH Benchmark Settings
 * *****************************************************************************
 */

lazy val TPC = config("tpc") extend Test
inConfig(TPC)(Defaults.testTasks)

TPC / sourceDirectory := baseDirectory.value / "src" / "test"
TPC / parallelExecution := false
TPC / fork := true
TPC / run / fork := true
TPC / run / javaOptions ++= {
  // Generate `java.hprof.txt` in the project root for very simple profiling
  // The feature was removed in JDK9, however for Spark we must support JDK8
  if (ManagementFactory.getRuntimeMXBean.getVmVersion.startsWith("1.8")) {
    List("-agentlib:hprof=cpu=samples")
  } else {
    Nil
  }
}
TPC / testOptions := {
  {
    if ((TPC / debugToHtml).value)
      Seq(
        Tests.Filter(tpcFilter),
        Tests.Argument("-C", "org.scalatest.tools.TrueHtmlReporter"),
        Tests.Argument("-Dmarkup=true")
      )
    else Seq(Tests.Filter(tpcFilter))
  } ++ {
    val doFailFast = (TPC / failFast).value
    Seq(Tests.Argument(s"-Dfailfast=${doFailFast}"))
  }
}

def tpcFilter(name: String): Boolean = name.startsWith("io.sparkcyclone.tpc")

val debugToHtml = SettingKey[Boolean]("debugToHtml")
debugToHtml := false

val failFast = SettingKey[Boolean]("failFast")
failFast := false


/**
 * *****************************************************************************
 * Acceptance Test Settings
 * *****************************************************************************
 */

lazy val AcceptanceTest = config("acc") extend Test
inConfig(AcceptanceTest)(Defaults.testTasks)

AcceptanceTest / parallelExecution := false
AcceptanceTest / testOptions := Seq(Tests.Filter(accFilter))
AcceptanceTest / testOptions += Tests.Argument("-Cio.sparkcyclone.acceptance.MarkdownReporter")
AcceptanceTest / testOptions += Tests.Argument("-o")

def accFilter(name: String): Boolean = name.startsWith("io.sparkcyclone.acceptance")


/**
 * *****************************************************************************
 * Command Aliases
 * *****************************************************************************
 */

addCommandAlias("check", ";scalafmtCheck;scalafmtSbtCheck;testQuick")

addCommandAlias("fmt", ";scalafmtSbt;scalafmtAll")

addCommandAlias("skipBenchTests", "; set `fun-bench` / Test / skip := true")

addCommandAlias("unskipBenchTests", "; set `fun-bench` / Test / skip := false")

addCommandAlias(
  "compile-all",
  "; Test / compile ; ve-direct / Test / compile ; ve-direct / It / compile"
)

addCommandAlias(
  "testQuick-all",
  "; Test / testQuick ; AcceptanceTest / testQuick ; VectorEngine / testQuick"
)

addCommandAlias(
  "test-all",
  "; Test / test ; AcceptanceTest / test ; VectorEngine / test"
)


/**
 * *****************************************************************************
 * Assembly / Deployment Settings
 * *****************************************************************************
 */

assembly / assemblyMergeStrategy := {
  case v if v.contains("module-info.class")   => MergeStrategy.discard
  case v if v.contains("UnusedStub")          => MergeStrategy.first
  case v if v.contains("aopalliance")         => MergeStrategy.first
  case v if v.contains("inject")              => MergeStrategy.first
  case v if v.contains("reflect-config.json") => MergeStrategy.discard
  case v if v.contains("jni-config.json")     => MergeStrategy.discard
  case v if v.contains("git.properties")      => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val deploy = inputKey[Unit]("Deploy artifacts to `deployTarget`")
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
    logger.info(s"Copying JAR locally to /opt/cyclone/${sys.env("USER")}/spark-cyclone-sql-plugin.jar:")
    Seq("cp", generatedFile.toString, s"/opt/cyclone/${sys.env("USER")}/spark-cyclone-sql-plugin.jar") ! logger
    logger.info(s"Copied.")
  } else {
    logger.info(s"Uploading JAR to ${targetBox}")
    Seq("ssh", targetBox, "mkdir", "-p", "/opt/cyclone/") ! logger
    Seq(
      "scp",
      generatedFile.toString,
      s"${targetBox}:/opt/cyclone/spark-cyclone-sql-plugin.jar"
    ) ! logger
    logger.info(s"Uploaded JAR")
  }
}

lazy val deployExamples = inputKey[Unit]("Deploy artifacts to `deployTarget`")
deployExamples := {
  val args: Seq[String] = spaceDelimited("<arg>").parsed
  val targetBox = args.headOption.getOrElse(sys.error("Deploy target missing"))
  val logger = streams.value.log
  import scala.sys.process._

  logger.info(s"Preparing deployment of examples to ${targetBox}...")
  Seq("ssh", targetBox, "mkdir", "-p", "/opt/cyclone/", "/opt/cyclone/examples/") ! logger
  logger.info("Created dir.")
  Seq(
    "scp",
    "-r",
    (baseDirectory.value / "examples").getAbsolutePath,
    s"${targetBox}:/opt/cyclone/"
  ) ! logger
  Seq(
    "scp",
    (baseDirectory.value / "README.md").getAbsolutePath,
    s"${targetBox}:/opt/cyclone/"
  ) ! logger
  logger.info("Uploaded examples.")
}


/**
 * *****************************************************************************
 * tracing Build Settings
 * *****************************************************************************
 */

lazy val tracing = project
  .enablePlugins(JavaServerAppPackaging)
  .enablePlugins(SystemdPlugin)
  .disablePlugins(org.bytedeco.sbt.javacpp.Plugin)
  .enablePlugins(RpmPlugin)
  .dependsOn(root % "test->test")
  .settings(
    rpmLicense := Some("Proprietary"),
    rpmVendor := "nec",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-io" % "3.1.5",
      "co.fs2" %% "fs2-core" % "3.1.5",
      "org.scalatest" %% "scalatest" % "3.2.10" % Test,
      "com.eed3si9n.expecty" %% "expecty" % "0.15.4" % Test
    ),
    name := "tracing",
    reStart := reStart.dependsOn((Test / testQuick).toTask("")).evaluated,
    reStart / envVars += "LOG_DIR" -> file("tracing-dir").getAbsolutePath
  )


/**
 * *****************************************************************************
 * fun-bench Build Settings
 * *****************************************************************************
 */

// Run with: fun-bench / Jmh / run -h
lazy val `fun-bench` = project
  .enablePlugins(JmhPlugin)
  .disablePlugins(org.bytedeco.sbt.javacpp.Plugin)
  .dependsOn(root % "compile->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "co.fs2" %% "fs2-io" % "3.0.6" % "test,acc,ve",
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
            (root / Test / testQuick).toTask("")
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
            (root / Test / runMain).toTask(s" io.sparkcyclone.spark.GenerateBenchmarksApp ${tgt}").value
            Seq(tgt)
          }
        )
      }
  )

val bench = inputKey[Unit]("Runs JMH benchmarks in fun-bench")
bench := (`fun-bench` / Jmh / run).evaluated


/**
 * *****************************************************************************
 * Tests/tpcbench Build Settings
 * *****************************************************************************
 */

lazy val tpchbench = project
  .in(file("tests/tpchbench"))
  .disablePlugins(org.bytedeco.sbt.javacpp.Plugin)
  .settings(
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
    scalacOptions ++= Seq("-Xfatal-warnings", "-feature", "-deprecation"),
    version := "0.0.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
    libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "0.38.2",
    libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    // test suite settings
    Test / fork := true,
    javaOptions ++= Seq("-Xms2G", "-Xmx32G", "-XX:+CMSClassUnloadingEnabled"),
    // Show runtime of tests
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
    // JAR file settings
    // don't include Scala in the JAR file
    // assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
    // Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
    // You can add the JAR file naming conventions by running the shell script
  )


/**
 * *****************************************************************************
 * Tpcbench-run Build Settings
 * *****************************************************************************
 */

lazy val `tpcbench-run` = project
  .disablePlugins(org.bytedeco.sbt.javacpp.Plugin)
  .settings(
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-io" % "3.0.6" % "test,acc,ve",
      "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC1",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC1",
      "org.scalatest" %% "scalatest" % "3.2.10" % Test,
      "io.circe" %% "circe-literal" % "0.14.1",
      "io.circe" %% "circe-generic" % "0.14.1",
      "io.circe" %% "circe-parser" % "0.14.1",
      "io.circe" %% "circe-optics" % "0.14.1",
      "org.http4s" %% "http4s-circe" % "0.23.7",
      "org.http4s" %% "http4s-scala-xml" % "0.23.7",
      "org.http4s" %% "http4s-dsl" % "0.23.7",
      "org.apache.commons" % "commons-lang3" % "3.10",
      "org.http4s" %% "http4s-blaze-client" % "0.23.7",
      "com.lihaoyi" %% "scalatags" % "0.11.0",
      "com.eed3si9n.expecty" %% "expecty" % "0.15.4" % Test,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.13.1",
      "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    ),
    run / fork := true,
    (Compile / run) := (Compile / run)
      .dependsOn((Test / testQuick).toTask(""))
      .dependsOn((root / Test / compile))
      .dependsOn((root / VectorEngine / compile))
      .evaluated,
    run / javaOptions ++= List(
      s"-Dve.package=${(tpchbench / assembly).value.absolutePath}",
      s"-Dve.cyclone_jar=${(root / assembly).value.absolutePath}"
    ),
    reStart / envVars += "PACKAGE" -> (tpchbench / assembly).value.absolutePath,
    reStart / envVars += "CYCLONE_JAR" -> (root / assembly).value.absolutePath,
    reStart := reStart
      .dependsOn((Test / testQuick).toTask(""))
      .dependsOn(root / Test / compile)
      .dependsOn((root / VectorEngine / compile))
      .evaluated,
    Test / fork := true
  )
  .dependsOn(tracing)


/**
 * *****************************************************************************
 * Cyclone C++ (VE) Library Build Settings
 * *****************************************************************************
 */

// Declare the C++ source and target directories
lazy val cycloneCppSrcDir = settingKey[File]("Cyclone C++ source directory")
lazy val cycloneCppTgtDir = settingKey[File]("Cyclone C++ target directory")
cycloneCppSrcDir := (Compile / resourceDirectory).value / "io" / "sparkcyclone" / "cpp"
cycloneCppTgtDir := (Compile / resourceManaged).value / "cycloneve"

Compile / resourceGenerators += Def.taskDyn {
  // If ncc is availble, build both the BOM and the library
  if (Files.exists(Paths.get("/opt/nec/ve/bin/ncc"))) buildCycloneCppLibrary.toTask
  else if (isWin) emptyTask.toTask
  else buildCycloneCppSourcesBomOnly.toTask
}.taskValue

lazy val emptyTask = taskKey[Seq[File]]("Do nothing")
emptyTask := {
  Seq.empty
}

lazy val cycloneCppLibrarySources = taskKey[Seq[File]]("Cyclone C++ library sources")
cycloneCppLibrarySources := {
  sbt.nio.file.FileTreeView.default
    .list(Seq("frovedis/**", "cyclone/**", "tests/**", "Makefile").map { suffix =>
      Glob((Compile / resourceDirectory).value.toString + s"/io/sparkcyclone/cpp/${suffix}")
    })
    .map(_._1.toFile)
}

lazy val copyCycloneCppSourcesToTarget = taskKey[Unit]("Copy Cyclone C++ sources to target directory")
copyCycloneCppSourcesToTarget := {
  val logger = streams.value.log

  logger.info(s"Creating target directory: ${cycloneCppTgtDir.value}...")
  IO.createDirectory(cycloneCppTgtDir.value)

  // Clear out any old source files left over from previous runs, if they exist
  if (
    (Process(
      Seq("rm", "-rf", s"${cycloneCppTgtDir.value}/*"),
      cycloneCppSrcDir.value
    ) ! logger) != 0
  ) {
    sys.error(s"Failed to clear target directory.")
  }

  logger.info(s"Copying C++ source files over to the target directory...")
  if (
    (Process(
      Seq("cp", "-R", ".", cycloneCppTgtDir.value.toString),
      cycloneCppSrcDir.value
    ) ! logger) != 0
  ) {
    sys.error(s"Failed to copy Cyclone C++ library files over.")
  }
}

lazy val buildCycloneCppSourcesBomOnly = taskKey[Seq[File]]("Create the Cyclone C++ sources BOM only")
buildCycloneCppSourcesBomOnly := {
  val logger = streams.value.log

  // Copy the files over to the target directory
  (copyCycloneCppSourcesToTarget.value: @sbtUnchecked)

  // Build the BOM directly in the target directory to avoid cache issues
  logger.info(s"Creating the C++ sources BOM...")
  if ((Process(Seq("make", "bom", "clean"), cycloneCppTgtDir.value) ! logger) != 0) {
    sys.error("Failed to create Cyclone C++ library sources BOM.")
  }

  Seq(new File(cycloneCppTgtDir.value, "sources.bom"))
}

def getAvailableVeNodes: Set[String] = {
  val lines = MSeq.empty[String]
  if ((Process(Seq("/opt/nec/ve/bin/ps", "aux")) ! ProcessLogger(lines += _, line => ())) != 0) {
    println("Failed to fetch VE information from ps")
  }

  val runningVeProcs = MMap.empty[String, MSeq[String]]
  var currentProcList = MSeq.empty[String]

  for (line <- lines) {
    if (line.startsWith("VE Node")) {
      val node = line.replace("VE Node: ", "")
      currentProcList = MSeq.empty[String]
      runningVeProcs += ((node, currentProcList))

    } else if (line.trim.startsWith("USER        PID") || line.trim.isEmpty) {
      ()

    } else {
      currentProcList += line
    }
  }

  runningVeProcs.filter(_._2.isEmpty).keys.toSet
}

lazy val buildCycloneCppLibrary = taskKey[Seq[File]]("Build and test the Cyclone C++ library")
buildCycloneCppLibrary := {
  val logger = streams.value.log

  val cachedFun = FileFunction.cached(streams.value.cacheDirectory / "cpp") { (in: Set[File]) =>
    in.find(_.toString.contains("Makefile")) match {
      case Some(makefile) =>
        // Copy the files over to the target directory
        (copyCycloneCppSourcesToTarget.value: @sbtUnchecked)

        // Select the VE node to run the tests
        val venode = sys.env.get("VE_NODE_NUMBER") match {
          case Some(node) =>
            logger.info(s"Using user-specified VE node ${node} to run Cyclone C++ tests.")
            node

          case None =>
            val nodes = getAvailableVeNodes
            val node = nodes.headOption.getOrElse("0")
            logger.info(s"Detected available VE nodes: ${nodes}; using node ${node} to run Cyclone C++ tests.")
            node
        }

        // Build the library directly in the target directory to avoid cache issues
        logger.info(s"Building and testing libcyclone.so...")
        if (
          (Process(
            Seq("make", "cleanall", "all", "test", "-j"),
            cycloneCppTgtDir.value,
            ("VE_NODE_NUMBER", venode)
          ) ! logger) != 0
        ) {
          sys.error("Failed to build libcyclone.so; please check the compiler logs.")
        }

        // Build the BOM directly in the target directory to avoid cache issues.
        // Must run in a subsequent process or else the `clean` task will overstep
        // earlier tasks if `make` is invoked with parallelization turned on.
        logger.info(s"Creating the C++ source BOM...")
        if (
          (Process(
            Seq("make", "bom", "clean"),
            cycloneCppTgtDir.value
          ) ! logger) != 0
        ) {
          sys.error("Failed to create Cyclone C++ library sources BOM.")
        }

        // Collect the header files and generated arfifacts
        val files = in.filter { fp =>
          Seq(".hpp", ".incl", ".incl1", ".inc2").exists(fp.toString.endsWith(_))
        } ++
          Set("libcyclone.so", "sources.bom").map { fname =>
            new File(makefile.getParentFile, fname)
          }

        // Rebase filepaths on the target directory
        files.flatMap { f => Path.rebase(cycloneCppSrcDir.value, cycloneCppTgtDir.value).apply(f) }

      case None =>
        sys.error("Could not find the Makefile for libcyclone")
    }
  }

  cachedFun(cycloneCppLibrarySources.value.toSet).toList.sortBy(_.toString.contains(".so"))
}
buildCycloneCppLibrary / logBuffered := false


/**
 * *****************************************************************************
 * Rddbench Build Settings
 * *****************************************************************************
 */

lazy val `rddbench` = project
  .dependsOn(root)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "org.scalatest" %% "scalatest" % "3.2.10" % Test
    ),
    version := "0.1"
  )


/**
 * *****************************************************************************
 * AVEOBench Build Settings
 * *****************************************************************************
 */

lazy val `aveobench` = project
  .dependsOn(root)
  .enablePlugins(JmhPlugin)
  .settings(
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value
    )
  )
