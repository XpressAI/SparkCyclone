package com.nec.h2

import java.io.InputStreamReader
import java.sql.{Connection, DriverManager}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scala.util.Random

import org.h2.tools.RunScript

import org.apache.spark.sql.SparkSession

object H2DatabaseConnector {
  val inputH2Url = "jdbc:h2:mem:inputDb;MODE=MYSQL;USER=sa;DB_CLOSE_DELAY=-1"
  val conn = DriverManager.getConnection(inputH2Url, "sa", "")

  def init(): Unit = {

    val resource = getClass.getResourceAsStream("init.sql")

    RunScript.execute(
      conn, new InputStreamReader(resource)
    )

  }

  def teardown(): Unit = {
    conn
      .createStatement()
      .execute("DROP TABLE Users")

    conn.close()
  }

}
