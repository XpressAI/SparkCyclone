package com.nec.debugging

import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SparkPlan

object Debugging {
  implicit class SprarkSessionImplicit(val sparkSession: SparkSession) {
    def debugSql(sqlQuery: String, name: String): DataFrame = {
      val plansDir = Paths.get("target", "plans")
      if(!plansDir.toFile.exists()) {
        Files.createDirectory(plansDir)
      }
      val frame = sparkSession.sql(sqlQuery)
      Files.write(
        Paths.get("target", "plans", name),
        frame.queryExecution.sparkPlan.toString().getBytes("UTF-8"),
        StandardOpenOption.CREATE
      )
      frame
    }
  }
}
