package com.nec.ve

import com.nec.aurora.Aurora
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec
import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK

object DynamicVeSqlExpressionEvaluationSpec {

  def VeConfiguration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .config("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession =>
          new VERewriteStrategy(ExecutorPluginManagedEvaluator)
        )
      )
  }

}

final class DynamicVeSqlExpressionEvaluationSpec extends DynamicCSqlExpressionEvaluationSpec {

  override def configuration: SparkSession.Builder => SparkSession.Builder =
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration

  override protected def afterAll(): Unit = {
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
  }

  override protected def beforeAll(): Unit = {
    Aurora4SparkExecutorPlugin._veo_proc = Aurora.veo_proc_create(-1)
    super.beforeAll()
  }
}
