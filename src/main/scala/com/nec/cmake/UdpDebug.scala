package com.nec.cmake

import com.nec.spark.agile.CExpressionEvaluation.CodeLines

object UdpDebug {

  final case class Always(sockName: String, destinationName: String, hostName: String, port: String) extends UdpDebug {
    override def headers: CodeLines = CodeLines.from(
      "#include <iostream>",
      "#include <string>",
      "#include <arpa/inet.h> // htons, inet_addr",
      "#include <netinet/in.h> // sockaddr_in",
      "#include <sys/types.h> // uint16_t",
      "#include <sys/socket.h> // socket, sendto",
      "#include <unistd.h> // close"
    )

    override def createSock: CodeLines = CodeLines.from(
      s"int ${sockName} = ::socket(AF_INET, SOCK_DGRAM, 0);",
      s"sockaddr_in ${destinationName};",
      s"${destinationName}.sin_family = AF_INET;",
      s"${destinationName}.sin_port = htons(${port});",
      s"""${destinationName}.sin_addr.s_addr = inet_addr(std::string("${hostName}").c_str());"""
    )

    override def close: CodeLines = CodeLines.from(s"::close(${sockName});")

    override def send(what: String*): CodeLines = CodeLines
      .from(
        "ostringstream s;",
        "s " + what.map(s => s"<< $s ") + ";",
        s"::sendto(${sockName}, s.str().c_str(), s.length(), 0, reinterpret_cast<sockaddr*>(&${destinationName}), sizeof(${destinationName}));"
      )
      .blockCommented("Send via UDP")
  }
}

trait UdpDebug {
  def headers: CodeLines
  def createSock: CodeLines
  def close: CodeLines
  def send(what: String*): CodeLines
}
