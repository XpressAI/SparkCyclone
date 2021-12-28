package sc

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import sc.DetectLogback.LogbackItemsClasspath
import sc.RunOptions.{cycloneJar, packageJar}

import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermission._
import java.util

final case class RunOptions(
  runId: String,
  kernelDirectory: Option[String],
  gitCommitSha: String,
  gitBranch: String,
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
  passThroughProject: Boolean,
  failFast: Boolean,
  exchangeOnVe: Boolean
) {

  def enhanceWithEnv(env: Map[String, String]): RunOptions = env
    .collect {
      case (k, v) if k.startsWith("INPUT_") =>
        k.drop("INPUT_".length) -> v
    }
    .foldLeft(this) { case (t, (k, v)) => t.setArg(k, v).getOrElse(t) }

  def enhanceWith(args: List[String]): RunOptions = {
    args
      .sliding(2)
      .foldLeft {
        args
          .foldLeft(this) { case (ro, arg) =>
            ro.rewriteArgs(arg).getOrElse(ro)
          }
      } {
        case (r, a :: b :: Nil) => r.rewriteArgsTwo(a, b).getOrElse(r)
        case (r, _)             => r
      }
  }

  def pluginBooleans: List[(String, String)] = {
    List(
      ("spark.com.nec.spark.aggregate-on-ve", aggregateOnVe),
      ("spark.com.nec.spark.sort-on-ve", enableVeSorting),
      ("spark.com.nec.spark.pass-through-project", passThroughProject),
      ("spark.com.nec.spark.project-on-ve", projectOnVe),
      ("spark.com.nec.spark.filter-on-ve", filterOnVe),
      ("spark.com.nec.spark.exchange-on-ve", exchangeOnVe),
      ("spark.com.nec.spark.fail-fast", failFast)
    ).map { case (k, v) => (k, v.toString) }
  }

  def includeExtra(e: String): RunOptions =
    copy(extras = Some((extras.toList ++ List(e)).mkString(" ")))

  def rewriteArgsTwo(a: String, b: String): Option[RunOptions] =
    PartialFunction.condOpt(a -> b) { case (k @ "--conf", v) =>
      includeExtra(s"$k $v")
    }

  def setArg(key: String, value: String): Option[RunOptions] = {
    PartialFunction.condOpt(key -> value) {
      case ("query", nqn) if nqn.forall(Character.isDigit) => copy(queryNo = nqn.toInt)
      case ("cyclone" | "use-cyclone", nqn)                => copy(useCyclone = nqn == "on" || nqn == "true")
      case ("scale", newScale)                             => copy(scale = newScale)
      case ("name", newName)                               => copy(name = Some(newName))
      case ("extra", e)                                    => includeExtra(e)
      case ("serializer", v)                               => copy(serializerOn = v == "on" || v == "true")
      case ("ve-log-debug", v)                             => copy(veLogDebug = v == "on" || v == "true")
      case ("pass-through-project", v)                     => copy(passThroughProject = v == "on" || v == "true")
      case ("fail-fast", v)                                => copy(failFast = v == "on" || v == "true")
      case ("filter-on-ve", v)                             => copy(filterOnVe = v == "on" || v == "true")
      case ("kernel-directory", newkd)                     => copy(kernelDirectory = Some(newkd))
    }
  }

  def rewriteArgs(str: String): Option[RunOptions] = {
    Option(str)
      .filter(_.startsWith("--"))
      .map(_.drop(2))
      .map(_.split('=').toList)
      .collect { case k :: v :: Nil =>
        k -> v
      }
      .flatMap { case (k, v) => setArg(k, v) }
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
      "cluster",
      "--conf",
      "spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc"
    ) ++ {
      if (useCyclone) {
        val exCls: String =
          (List(cycloneJar) ++ LogbackItemsClasspath.map(_.getFileName.toString)).mkString(":")
        List(
          "--jars",
          (List(cycloneJar) ++ LogbackItemsClasspath.map(_.toString)).mkString(","),
          "--conf",
          s"spark.executor.extraClassPath=${exCls}",
          "--conf",
          s"spark.driver.extraClassPath=${exCls}",
          "--conf",
          "spark.plugins=com.nec.spark.AuroraSqlPlugin"
        )
      } else Nil
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

  lazy val fieldNames: List[String] = {
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

  lazy val default: RunOptions = RunOptions(
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
      IO.blocking { List("git", "rev-parse", "HEAD").!!.trim.take(8) }
        .handleError(_ => "")
        .unsafeRunSync()
    },
    gitBranch = {
      import scala.sys.process._
      IO.blocking { List("git", "rev-parse", "--abbrev-ref", "HEAD").!!.trim }
        .handleError(_ => "")
        .unsafeRunSync()
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
    useCyclone = true,
    passThroughProject = false,
    failFast = true
  )

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
