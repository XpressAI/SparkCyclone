package com.nec.spark.planning

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.nec.spark.{
  RequestCompiledLibraryForCode,
  RequestCompiledLibraryResponse,
  SparkCycloneExecutorPlugin
}

import org.apache.spark.api.plugin.PluginContext

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
          RequestCompiledLibraryForCode(libraryPath)
        ) match {
          case RequestCompiledLibraryResponse(bytes) =>
            Files.write(
              path,
              bytes.toByteArray,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE
            )
        }
      }

      tmp.normalize.toAbsolutePath
    }
  }
}
