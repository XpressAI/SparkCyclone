package com.nec.spark.planning

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.nec.spark.{RequestCompiledLibraryForCode, RequestCompiledLibraryResponse, SparkCycloneExecutorPlugin}

import org.apache.spark.api.plugin.PluginContext

object LibLocation {
  trait LibLocation {
    def resolveLocation(): Path
  }

  case class LocalLibLocation(libraryPath: String) extends LibLocation {
    override def resolveLocation(): Path ={
      if(Files.exists(Paths.get(libraryPath))) {
        throw new RuntimeException(s"Expected library to be present in ${libraryPath}, but file does not exist")
      } else {
        Paths.get(libraryPath).toAbsolutePath
      }
    }
  }

  case class DistributedLibLocation(libraryPath: String, code: String) extends LibLocation {
    override def resolveLocation(): Path = {
      val path = Paths.get(libraryPath)
      if(Files.exists(path)){
        path.toAbsolutePath
      } else {
        SparkCycloneExecutorPlugin.pluginContext.ask(RequestCompiledLibraryForCode(code)) match {
          case RequestCompiledLibraryResponse(bytez) =>
            Files.write(path, bytez.toByteArray, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        }
      }
    }
  }

}
