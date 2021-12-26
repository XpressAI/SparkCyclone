package sc

object LogbackBenchmarkFile {
  def forPort(port: Int): scala.xml.Elem = <configuration debug="true">
    <appender name="SERVER" class="ch.qos.logback.classic.net.server.SocketAppender">
      <remoteHost>127.0.0.1</remoteHost>
      <port>${port}</port>
      <reconnectionDelay>10000</reconnectionDelay>
      <includeCallerData>true</includeCallerData>
    </appender>
    <root level="info">
      <appender-ref ref="SERVER"/>
    </root>
  </configuration>
}
