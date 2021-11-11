/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
  ): Int = {
    val buf = Array.ofDim[Byte](bufSize)
    var hasMore = true
    val byteBuffer = ByteBuffer.allocate(4)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
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
        byteBuffer.position(0)
        byteBuffer.putInt(bytesRead)
        byteBuffer.position(0)
        logger.whenTraceEnabled {
          logger.trace(s"Writing ${bytesRead} bytes to client...")
        }
        outputStream.write(byteBuffer.array())
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

      byteBuffer.position(0)
      byteBuffer.putInt(0)
      byteBuffer.position(0)
      logger.debug("Ending stream")
      outputStream.write(byteBuffer.array())
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
      val totalTransferred =
        try inputStreamToOutputStream(inputStream, sockie.getOutputStream, bufSize)
        finally sockie.close()
      val endTime = System.currentTimeMillis()
      logger.debug(
        s"Transfer out time = ${endTime - startTime}ms, total was ${totalTransferred} bytes (${totalTransferred / (endTime - startTime)}K/s)"
      )
    }

    (socketName, serverSocket)
  }
}
