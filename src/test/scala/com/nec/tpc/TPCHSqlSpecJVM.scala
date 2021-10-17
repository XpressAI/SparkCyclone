package com.nec.tpc

import com.nec.spark.SparkAdditions
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}


class TPCHSqlSpecJVM
  extends TPCHSqlCSpec
  with BeforeAndAfter
  with BeforeAndAfterAll
  with SparkAdditions
  with Matchers
  with LazyLogging {

  private var initialized = false

  override def configuration: SparkSession.Builder => SparkSession.Builder =
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)

}
