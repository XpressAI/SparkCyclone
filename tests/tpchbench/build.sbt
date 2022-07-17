name := "tpchbench"
version := "0.0.1"
scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.github.mrpowers" %% "spark-daria" % "0.38.2",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

javaOptions ++= Seq("-Xms2G", "-Xmx32G", "-XX:+CMSClassUnloadingEnabled")

Test / fork := true
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
