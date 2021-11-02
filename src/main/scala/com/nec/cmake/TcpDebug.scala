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
package com.nec.cmake

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.planning.Tracer

object TcpDebug {

  def conditional: TcpDebug = Conditional(default.hostName, default)

  def conditionOn(define: String)(code: CodeLines): CodeLines =
    CodeLines.from(s"#ifdef ${define}", code, "#endif")

  final case class Conditional(name: String, underlying: TcpDebug) extends TcpDebug {

    override def headers: CodeLines =
      conditionOn(name)(underlying.headers)

    override def createSock: CodeLines =
      conditionOn(name)(underlying.createSock)

    override def close: CodeLines =
      conditionOn(name)(underlying.close)

    override def send(what: String*): CodeLines =
      conditionOn(name)(underlying.send(what: _*))
  }

  def default: Always = Always("profile_sock", "profile_sock_dest", "PROFILE_HOST", "PROFILE_PORT")

  final case class Always(sockName: String, destinationName: String, hostName: String, port: String)
    extends TcpDebug {
    override def headers: CodeLines = CodeLines.from(
      "#include <iostream>",
      "#include <string>",
      "#include <sstream>",
      "#include <arpa/inet.h>",
      "#include <netinet/in.h>",
      "#include <sys/types.h>",
      "#include <sys/socket.h>",
      "#include <unistd.h>"
    )

    override def createSock: CodeLines = CodeLines.from(
      s"int ${sockName} = ::socket(AF_INET, SOCK_STREAM, 0);",
      s"sockaddr_in ${destinationName};",
      s"${destinationName}.sin_family = AF_INET;",
      s"${destinationName}.sin_port = htons(${port});",
      s"""${destinationName}.sin_addr.s_addr = inet_addr(std::string(${hostName}).c_str());""",
      s"if (connect(${sockName}, (struct sockaddr*)&${destinationName}, sizeof(${destinationName})) != 0) {",
      s"""  std::cout << "error connecting..." << std::endl << std::flush;""",
      "}"
    )

    override def close: CodeLines = CodeLines.from(s"::close(${sockName});")

    override def send(what: String*): CodeLines = CodeLines
      .from(
        "std::ostringstream s;",
        "s " + Tracer.concatStr(what.toList) + ";",
        s"write(${sockName}, s.str().c_str(), s.str().length());"
      )
      .blockCommented("Send via TCP")
  }
}

trait TcpDebug {
  def headers: CodeLines
  def createSock: CodeLines
  def close: CodeLines
  def send(what: String*): CodeLines
}
