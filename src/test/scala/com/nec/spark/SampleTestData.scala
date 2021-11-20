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
import java.nio.file.Paths
import java.nio.file.Path

object SampleTestData {
  lazy val SampleStrCsv: Path = Paths
    .get(
      this.getClass
        .getResource("sample-str.csv")
        .toURI
    )
    .toAbsolutePath

  lazy val SampleStrCsv2: Path = Paths
    .get(
      this.getClass
        .getResource("sample-str-2.csv")
        .toURI
    )
    .toAbsolutePath

  lazy val SampleMultiColumnCSV: Path = PkgDir.resolve("sampleMultiColumn-distributedCsv")
  lazy val SecondSampleMultiColumnCsv: Path = PkgDir.resolve("sampleMultiColumn-distributedCsv2")
  lazy val ConvertedParquet: Path = PkgDir.resolve("convertedSample.parquet")
  lazy val ConvertedJoinTable: Path = PkgDir.resolve("convertedJoinTable.snappy.parquet")

  /** When forked, this is no longer an external file, but a resource * */
  lazy val PkgDir = {

    /** When running from fun-bench we need to look 1 directory up */
    val root =
      if (Paths.get(".").toAbsolutePath.toString.contains("fun-bench"))
        Paths
          .get("../src/test/resources")
      else
        Paths
          .get("src/test/resources")

    root
      .resolve(
        this.getClass.getPackage.getName
          .replaceAllLiterally(".", "/")
      )
      .toAbsolutePath
  }

  lazy val SampleTwoColumnParquet: Path =
    PkgDir.resolve("sampleMultiColumnParquet2.parquet")

  lazy val SampleTwoColumnParquetNonNull: Path =
    PkgDir.resolve("sampleMultiColumnParquet2NoNulls.parquet")

  lazy val OrdersCsv: Path =
    PkgDir.resolve("orders.csv")

  lazy val SampleCSV: Path =
    PkgDir.resolve("sample.csv")

  lazy val SampleDateCSV: Path =
    PkgDir.resolve("sample-date.csv")

  lazy val SampleDateCSV2: Path =
    PkgDir.resolve("sample-date-shipments.csv")

}
