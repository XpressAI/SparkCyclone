package com.nec.native

import com.typesafe.scalalogging.LazyLogging

import java.io.InputStream
import org.scalasbt.ipcsocket.UnixDomainServerSocket
import org.scalasbt.ipcsocket.UnixDomainSocket

import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object IpcTransfer extends LazyLogging {

  def inputStreamToOutputStream(
    inputStream: InputStream,
    outputStream: OutputStream,
    bufSize: Int
  ): Int = {
    val buf = Array.ofDim[Byte](bufSize)
    var hasMore = true
    logger.whenTraceEnabled {
      logger.debug(s"Preparing to read input stream")
    }
    var totalTransferred = 0
    try {
      var bytesRead = inputStream.read(buf)
      if (bytesRead < 1) {
        hasMore = false
      }
      logger.whenTraceEnabled {
        logger.debug(s"Read ${bytesRead} bytes (initially)")
      }
      while (hasMore) {
        totalTransferred += bytesRead
        logger.whenTraceEnabled {
          logger.trace(s"Writing ${bytesRead} bytes to client...")
        }
        outputStream.write(buf, 0, bytesRead)
        outputStream.flush()
        bytesRead = inputStream.read(buf)
        if (bytesRead < 1) {
          hasMore = false
        }
        logger.whenTraceEnabled {
          logger.trace(s"Read ${bytesRead} bytes.")
        }
      }
      outputStream.flush()
      logger.debug("Ended stream.")
      totalTransferred
    } finally {
      outputStream.close()
      inputStream.close()
    }
  }

  def newServerSocket(socketName: String): ServerSocket =
    new UnixDomainServerSocket(socketName, false)

  def newClientSocket(socketName: String): Socket = new UnixDomainSocket(socketName, false)

  def transferIPC(inputStream: InputStream, bufSize: Int): (String, ServerSocket) = {
    val socketName = s"/tmp/test-sock-${scala.util.Random.nextInt()}"
    logger.debug("Launching IPC server socket...")
    val serverSocket = newServerSocket(socketName)
    val serverRespond = Future.apply {
      logger.debug("Waiting for a connection...")
      val sockie = serverSocket.accept()
      logger.debug(s"Got a client connection! ${sockie}; beginning transfer")
      val startTime = System.currentTimeMillis()
      val totalTransferred = try inputStreamToOutputStream(inputStream, sockie.getOutputStream, bufSize)
      finally sockie.close()
      val endTime = System.currentTimeMillis()
      logger.debug(s"Transfer out time = ${endTime - startTime}ms, total was ${totalTransferred} bytes (${totalTransferred / (endTime - startTime)}K/s)")
    }

    (socketName, serverSocket)
  }
}
