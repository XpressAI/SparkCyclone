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
package com.nec.h2

import java.io.InputStreamReader
import java.sql.{Connection, DriverManager}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scala.util.Random

import org.h2.tools.RunScript

import org.apache.spark.sql.SparkSession

object H2DatabaseConnector {
  val inputH2Url = "jdbc:h2:mem:inputDb;USER=sa;DB_CLOSE_DELAY=-1"
  val conn = {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection(inputH2Url, "sa", "")
  }

  def init(): Unit = {

    val resource = getClass.getResourceAsStream("init.sql")

    RunScript.execute(conn, new InputStreamReader(resource))

  }

  def teardown(): Unit = {
    conn
      .createStatement()
      .execute("DROP TABLE Users")

    conn.close()
  }

}
