package com.nec.tpc

import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.AuroraSqlPlugin
import com.nec.spark.planning.VERewriteStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK

object TPCHSqlSpec {

  def VeConfiguration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .config("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(_ => new VERewriteStrategy(ExecutorPluginManagedEvaluator))
      )
  }

}
