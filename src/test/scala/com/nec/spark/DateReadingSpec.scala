package com.nec.spark

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.scalatest.freespec.AnyFreeSpec

import java.sql.Date

final class DateReadingSpec extends AnyFreeSpec {
  "It works" in {
    assert(DateTimeUtils.fromJavaDate(Date.valueOf("2010-01-01")) == 14610)
  }
  "It works one day later" in {
    assert(DateTimeUtils.fromJavaDate(Date.valueOf("2010-01-02")) == 14611)
  }
}
