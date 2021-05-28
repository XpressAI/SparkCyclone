package com.nec
import java.nio.file.Path
import java.nio.file.Paths

package object cmake {
  lazy val CMakeListsTXT: Path = Paths
    .get(
      this.getClass
        .getResource("CMakeLists.txt")
        .toURI
    )
    .toAbsolutePath
}
