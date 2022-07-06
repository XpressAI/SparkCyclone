package io.sparkcyclone.benchmarks

object BenchmarkLogbackConfigurationFile {
  def forPort(port: Int): scala.xml.Elem = <configuration debug="true">
    <appender name="SERVER" class="ch.qos.logback.classic.net.SocketAppender">
      <remoteHost>127.0.0.1</remoteHost>
      <port>{port}</port>
      <reconnectionDelay>10000</reconnectionDelay>
      <includeCallerData>true</includeCallerData>
    </appender>
    <root level="info">
      <appender-ref ref="SERVER"/>
    </root>
    <logger level="debug" name="io.sparkcyclone"/>
    <logger level="info" name="io.sparkcyclone.tpch"/>
  </configuration>
}
