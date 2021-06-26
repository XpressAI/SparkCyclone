package com.nec.debugging

import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.DatasetEnricher.RichDataSetString
import org.apache.spark.sql.execution.SparkPlan

import java.nio.file.StandardOpenOption

object Debugging {
  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSql(name: String): Dataset[T] = {
      val plansDir = Paths.get("target", "plans")
      if (!plansDir.toFile.exists()) {
        Files.createDirectory(plansDir)
      }
      val target = plansDir.resolve(s"$name.txt")
      dataSet.queryExecution.debug.toFile(target.toAbsolutePath.toString)
      dataSet
    }
    def debugSqlAndShow(name: String): Dataset[T] = {
      val plansDir = Paths.get("target", "plans")
      if (!plansDir.toFile.exists()) {
        Files.createDirectory(plansDir)
      }
      val target = plansDir.resolve(s"$name.txt")
      dataSet.queryExecution.debug.toFile(target.toAbsolutePath.toString)
      Files.write(
        target,
        s"\n\n${dataSet.showAsString()}\n".getBytes("UTF-8"),
        StandardOpenOption.APPEND
      )
      dataSet
    }
  }

  implicit class RichSparkPlan[T](val sparkPlan: SparkPlan) {
    def debugCodegen(name: String): SparkPlan = {
      val plansDir = Paths.get("target", "plans")
      if (!plansDir.toFile.exists()) {
        Files.createDirectory(plansDir)
      }
      val target = plansDir.resolve(s"$name.txt")
      Files.write(
        target,
        org.apache.spark.sql.execution.debug.codegenString(sparkPlan).getBytes("UTF-8")
      )
      sparkPlan
    }
  }

}
