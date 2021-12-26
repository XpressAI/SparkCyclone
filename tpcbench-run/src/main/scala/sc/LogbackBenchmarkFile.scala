package sc

object LogbackBenchmarkFile {
  def forPort(port: Int): scala.xml.Elem = <configuration debug="true">
    <appender name="SERVER" class="ch.qos.logback.classic.net.server.ServerSocketAppender">
      <port>${port}</port>
      <includeCallerData>true</includeCallerData>
    </appender>
    <root level="info">
      <appender-ref ref="SERVER"/>
    </root>
  </configuration>
}
