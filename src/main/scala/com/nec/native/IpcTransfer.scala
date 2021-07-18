package com.nec.native

import com.typesafe.scalalogging.LazyLogging

import java.io.InputStream
import org.scalasbt.ipcsocket.UnixDomainServerSocket
import org.scalasbt.ipcsocket.UnixDomainSocket

import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object IpcTransfer extends LazyLogging {

  def inputStreamToOutputStream(
    inputStream: InputStream,
    outputStream: OutputStream,
    bufSize: Int
  ): Unit = {
    val buf = Array.ofDim[Byte](bufSize)
    var hasMore = true
    val byteBuffer = ByteBuffer.allocate(4)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    var bytesRead = inputStream.read(buf)
    if (bytesRead < 1) {
      hasMore = false
    }
    while (hasMore) {
      byteBuffer.position(0)
      byteBuffer.putInt(bytesRead)
      byteBuffer.position(0)
      println("Writing to client...")
      outputStream.write(byteBuffer.array())
      outputStream.write(buf, 0, bytesRead)
      outputStream.flush()
      bytesRead = inputStream.read(buf)
      if (bytesRead < 1) {
        hasMore = false
      }
    }

    byteBuffer.position(0)
    byteBuffer.putInt(0)
    byteBuffer.position(0)
    println("Writing to client...")
    outputStream.write(byteBuffer.array())
    outputStream.flush()
    outputStream.close()
    println("Ended writing to client.")
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
      try inputStreamToOutputStream(inputStream, sockie.getOutputStream, bufSize)
      finally sockie.close()
      val endTime = System.currentTimeMillis()
      logger.debug(s"Transfer out time = ${endTime - startTime}ms")
    }

    (socketName, serverSocket)
  }
}
