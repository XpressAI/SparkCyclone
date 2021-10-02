package com.nec.spark

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.{Customer, Lineitem, Nation, Order, Part, Partsupp, Region, Supplier, TPCHSqlCSpec}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Ignore}

import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.{DataTypes, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}


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
