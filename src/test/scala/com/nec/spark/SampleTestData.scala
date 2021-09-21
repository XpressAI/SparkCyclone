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
