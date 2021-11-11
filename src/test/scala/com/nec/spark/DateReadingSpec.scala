/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
