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
        this
          .getClass()
          .getPackage
          .getName
          .replaceAllLiterally(".", "/")
      )
      .toAbsolutePath()
  }

  lazy val SampleTwoColumnParquet: Path =
    PkgDir.resolve("sampleMultiColumnParquet2.parquet")

  lazy val LargeParquet: Path =
    Path.of("/home/william/large-sample-parquet-10_9/")

  lazy val OrdersCsv: Path =
    PkgDir.resolve("orders.csv")

  lazy val SampleCSV: Path =
    PkgDir.resolve("sample.csv")

  lazy val LargeCSV: Path =
    Path.of("/home/dominik/large-sample-csv-10_9/")
}
