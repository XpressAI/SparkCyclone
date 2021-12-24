package sc

import sc.RunOptions.{cycloneJar, packageJar, Log4jFile}
import sun.misc.IOUtils

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.util

final case class RunOptions(
  runId: String,
  kernelDirectory: Option[String],
  gitCommitSha: String,
  queryNo: Int,
  useCyclone: Boolean,
  name: Option[String],
  numExecutors: Int,
  executorCores: Int,
  executorMemory: String,
  scale: String,
  offHeapEnabled: Boolean,
  columnBatchSize: Int,
  serializerOn: Boolean,
  veLogDebug: Boolean,
  codeDebug: Boolean,
  extras: Option[String],
  aggregateOnVe: Boolean,
  enableVeSorting: Boolean,
  projectOnVe: Boolean,
  filterOnVe: Boolean,
  exchangeOnVe: Boolean
) {

  def pluginBooleans: List[(String, String)] = {
    List(
      ("spark.com.nec.spark.aggregate-on-ve", aggregateOnVe),
      ("spark.com.nec.spark.sort-on-ve", enableVeSorting),
      ("spark.com.nec.spark.project-on-ve", projectOnVe),
      ("spark.com.nec.spark.filter-on-ve", filterOnVe),
      ("spark.com.nec.spark.exchange-on-ve", exchangeOnVe)
    ).map { case (k, v) => (k, v.toString) }
  }

  def includeExtra(e: String): RunOptions =
    copy(extras = Some((extras.toList ++ List(e)).mkString(" ")))

  def rewriteArgsTwo(a: String, b: String): Option[RunOptions] =
    PartialFunction.condOpt(a -> b) { case (k @ "--conf", v) =>
      includeExtra(s"$k $v")
    }

  def rewriteArgs(str: String): Option[RunOptions] = {
    Option(str)
      .filter(_.startsWith("--"))
      .map(_.drop(2))
      .map(_.split('=').toList)
      .collect { case k :: v :: Nil =>
        k -> v
      }
      .collect {
        case ("query", nqn) if nqn.forall(Character.isDigit) => copy(queryNo = nqn.toInt)
        case ("cyclone", nqn)                                => copy(useCyclone = nqn == "on")
        case ("scale", newScale)                             => copy(scale = newScale)
        case ("name", newName)                               => copy(name = Some(newName))
        case ("serializer", v)                               => copy(serializerOn = v == "on")
        case ("ve-log-debug", v)                             => copy(veLogDebug = v == "on")
        case ("kernel-directory", newkd)                     => copy(kernelDirectory = Some(newkd))
      }
      .orElse {
        val extraStr = "--extra="
        Option(str)
          .filter(_.startsWith(extraStr))
          .map(_.drop(extraStr.length))
          .map(includeExtra)
      }
  }

  def toArguments: List[String] = {

    /** https://www.hpc.nec/documents/veos/en/aveo/md_GettingStarted.html * */
    (if (veLogDebug) List("--conf", "spark.executorEnv.VEO_LOG_DEBUG=1") else Nil) ++ List(
      "--master",
      "yarn",
      s"--num-executors=$numExecutors",
      s"--executor-cores=$executorCores",
      s"--executor-memory=${executorMemory}",
      "--deploy-mode",
      "client",
      "--conf",
      s"spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${Log4jFile.toString}",
      "--conf",
      s"spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${Log4jFile.toString}",
      "--files",
      Log4jFile.toString,
      "--conf",
      "spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc"
    ) ++ {
      if (useCyclone)
        List(
          "--jars",
          cycloneJar,
          "--conf",
          s"spark.executor.extraClassPath=${cycloneJar}",
          "--conf",
          "spark.plugins=com.nec.spark.AuroraSqlPlugin"
        )
      else Nil
    } ++ List(
      "--conf",
      s"spark.sql.columnVector.offheap.enabled=${offHeapEnabled.toString}",
      "--conf",
      s"spark.com.nec.spark.ve.columnBatchSize=${columnBatchSize}",
      "--conf",
      s"spark.executor.resource.ve.amount=1",
      "--conf",
      s"spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh",
      "--conf",
      "spark.executorEnv.VE_OMP_NUM_THREADS=1"
    ) ++ {
      if (codeDebug)
        List("--conf", "spark.com.nec.spark.ncc.debug=true")
      else Nil
    } ++ pluginBooleans.flatMap { case (k, v) => List("--conf", s"$k=$v") } ++ extras.toList
      .flatMap(_.split(" "))
      .filter(_.nonEmpty) ++ kernelDirectory.toList.flatMap(kd =>
      List("--conf", s"spark.com.nec.spark.kernel.directory=${kd}")
    ) ++ {
      if (serializerOn && useCyclone)
        List("--conf", "spark.sql.cache.serializer=com.nec.spark.planning.VeCachedBatchSerializer")
      else Nil
    } ++ name.toList.flatMap(n => List("--name", n)) ++ List(packageJar) ++ List(
      s"hdfs://localhost:9000/user/github/dbgen${scale}"
    ) ++ List(s"--select=$queryNo")
  }
}

object RunOptions {
  val fieldNames: List[String] = {
    classOf[RunOptions].getDeclaredFields.map(_.getName).toList
  }

  lazy val packageJar: String =
    sys.env
      .getOrElse(
        "PACKAGE",
        sys.props.getOrElse(
          "ve.package",
          sys.error("Expected 'PACKAGE' env to specify the JAR to use to run")
        )
      )

  lazy val cycloneJar: String =
    sys.env.getOrElse(
      "CYCLONE_JAR",
      sys.props.getOrElse("ve.cyclone_jar", sys.error("Expected 'CYCLONE_JAR' to be passed."))
    )

  val default: RunOptions = RunOptions(
    numExecutors = 8,
    executorCores = 1,
    executorMemory = "8G",
    scale = "1",
    offHeapEnabled = true,
    columnBatchSize = 50000,
    serializerOn = true,
    queryNo = 1,
    name = None,
    gitCommitSha = {
      import scala.sys.process._
      List("git", "rev-parse", "HEAD").!!.trim.take(8)
    },
    veLogDebug = false,
    runId = "test",
    kernelDirectory = None,
    extras = None,
    aggregateOnVe = true,
    enableVeSorting = true,
    projectOnVe = true,
    filterOnVe = true,
    exchangeOnVe = true,
    codeDebug = false,
    useCyclone = true
  )

  lazy val Log4jFile: java.nio.file.Path = {
    val tempFile = {
      if (scala.util.Properties.isWin)
        Files.createTempFile("log4j", ".properties")
      else
        Files.createTempFile(
          "log4j",
          ".properties",
          PosixFilePermissions.asFileAttribute(PosixPermissions)
        )
    }
    Files
      .write(
        tempFile,
        IOUtils.readAllBytes(getClass.getResourceAsStream(s"/log4j-benchmark.properties"))
      )
    tempFile
  }

  import scala.collection.JavaConverters._
  lazy val PosixPermissions: util.Set[PosixFilePermission] = Set[PosixFilePermission](
    OWNER_READ,
    OWNER_WRITE,
    OWNER_EXECUTE,
    GROUP_READ,
    GROUP_EXECUTE,
    OTHERS_READ,
    OTHERS_EXECUTE
  ).asJava
}
