package com.nec.spark.agile

import java.nio.file.{Path, Paths}

object ReferenceData {
  lazy val SampleMultiColumnCSV: Path = Paths
    .get(
      this.getClass
        .getResource("/sampleMultiColumn.csv")
        .toURI
    )
    .toAbsolutePath
  lazy val SampleTwoColumnParquet: Path = Paths
    .get(
      this.getClass
        .getResource("/sampleMultiColumnParquet2.parquet")
        .toURI
    )
    .toAbsolutePath
  lazy val SampleCSV: Path = Paths
    .get(
      this.getClass
        .getResource("/sample.csv")
        .toURI
    )
    .toAbsolutePath
}
