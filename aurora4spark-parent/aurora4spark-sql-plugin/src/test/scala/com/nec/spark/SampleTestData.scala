package com.nec.spark
import java.nio.file.Paths
import java.nio.file.Path

object SampleTestData {
  lazy val SampleTXT: Path = Paths
    .get(
      this.getClass
        .getResource("sample.txt")
        .toURI
    )
    .toAbsolutePath

  lazy val SampleMultiColumnCSV: Path = Paths
    .get(
      this.getClass
        .getResource("sampleMultiColumn.csv")
        .toURI
    )
    .toAbsolutePath

  /** When forked, this is no longer an external file, but a resource * */
  lazy val PkgDir = Paths
    .get("src/test/resources")
    .resolve(
      this
        .getClass()
        .getPackage
        .getName
        .replaceAllLiterally(".", "/")
    )
    .toAbsolutePath()

  lazy val SampleTwoColumnParquet: Path = {
    PkgDir.resolve("sampleMultiColumnParquet2.parquet")
  }

  lazy val OrdersCsv: Path = {
    PkgDir.resolve("orders.csv")
  }

  lazy val SampleCSV: Path = Paths
    .get(
      this.getClass
        .getResource("sample.csv")
        .toURI
    )
    .toAbsolutePath
}
