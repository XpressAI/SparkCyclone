package sc

import java.net.URLClassLoader

object DetectLogback {

  private val ExpectedClassPathItems =
    List("logback-classic", "logback-core", "log4j-over")

  lazy val LogbackItemsClasspath: List[String] =
    ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .filter(item => ExpectedClassPathItems.exists(expected => item.toString.contains(expected)))
      .map(item => item.toString.replaceAllLiterally("file:/", "/"))
      .toList

}
