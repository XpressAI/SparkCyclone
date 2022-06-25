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
      if (Files.exists(path)) {
        path.toAbsolutePath
      } else {
        SparkCycloneExecutorPlugin.pluginContext.ask(
          RequestCompiledLibraryForCode(libraryPath)
        ) match {
          case RequestCompiledLibraryResponse(bytez) =>
            Files.write(
              path,
              bytez.toByteArray,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE
            )
        }
      }
    }
  }

}
