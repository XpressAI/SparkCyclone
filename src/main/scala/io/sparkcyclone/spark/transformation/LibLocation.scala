package io.sparkcyclone.spark.transformation

import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

object LibLocation {
  trait LibLocation {
    def resolve: Path
  }

  case class DistributedLibLocation(libraryPath: String) extends LibLocation {
    override def resolve: Path = {
      val path = Paths.get(libraryPath)

      val tmp = if (Files.exists(path)) {
        path

      } else {
        SparkCycloneExecutorPlugin.pluginContext.ask(
          RequestCompiledLibrary(libraryPath)
        ) match {
          case RequestCompiledLibraryResponse(bytes) =>
            Files.write(
              path,
              bytes.toArray,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE
            )
        }
      }

      tmp.normalize.toAbsolutePath
    }
  }
}
