package sc

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
    <logger level="info" name="com.nec.spark"/>
    <logger level="info" name="sparkcyclone.tpch"/>
  </configuration>
}
