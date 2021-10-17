package com.nec.tpc

import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import com.nec.ve.DynamicVeSqlExpressionEvaluationSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK

import java.io.File

object TPCHVESqlSpec {

  def VeConfiguration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .config("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(_ => new VERewriteStrategy(ExecutorPluginManagedEvaluator))
      )
  }

}

final class TPCHVESqlSpec extends TPCHSqlCSpec {

  private var initialized = false

  override def configuration: SparkSession.Builder => SparkSession.Builder =
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration

  override protected def afterAll(): Unit = {
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
  }

  override protected def beforeAll(): Unit = {

//    Aurora4SparkExecutorPlugin._veo_proc = Aurora.veo_proc_create(-1)

    val dbGenFile = new File("src/test/resources/dbgen/dbgen")
    if (!dbGenFile.exists()) {

//      s"cd ${dbGenFile.getParent} && make && ./dbgen".!
    }

    val tableFile = new File("src/test/resoruces/dbgen/lineitem.tbl")
    if (!tableFile.exists()) {

//      s"cd ${dbGenFile.getParent} && ./dbgen && popd".!
    }

    super.beforeAll()
  }

}
