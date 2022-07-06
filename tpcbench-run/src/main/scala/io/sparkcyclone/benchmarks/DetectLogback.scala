package io.sparkcyclone.benchmarks

import java.net.URLClassLoader
import java.nio.file.{Path, Paths}

object DetectLogback {

  private val ExpectedClassPathItems =
    List("logback-classic", "logback-core", "log4j-over")

  lazy val LogbackItemsClasspath: List[Path] =
    ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .filter(item => ExpectedClassPathItems.exists(expected => item.toString.contains(expected)))
      .map(item =>
        Paths.get(item.toString.replaceAllLiterally("file:/", "/").replaceAll("/C:", "C:"))
      )
      .toList

}
