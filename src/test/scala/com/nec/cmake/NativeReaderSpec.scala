package com.nec.cmake

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.StringWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.VarCharVectorWrapper
import com.nec.cmake.NativeReaderSpec.dataISunixSocketToNativeToArrow
import com.nec.cmake.NativeReaderSpec.newClientSocket
import com.nec.cmake.NativeReaderSpec.newServerSocket
import com.nec.cmake.NativeReaderSpec.unixSocketToNativeToArrow
import com.nec.cmake.ReadFullCSVSpec.samplePartedCsv
import com.nec.native.NativeEvaluator
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.util.Text
import org.apache.spark.WholeTextFileNativeRDD.RichSparkContext
import org.scalatest.freespec.AnyFreeSpec

import java.net.ServerSocket
import java.net.Socket
import org.scalasbt.ipcsocket.UnixDomainServerSocket
import org.scalasbt.ipcsocket.UnixDomainSocket
import org.scalasbt.ipcsocket.Win32NamedPipeServerSocket
import org.scalasbt.ipcsocket.Win32NamedPipeSocket
import org.scalasbt.ipcsocket.Win32SecurityLevel
import org.scalatest.BeforeAndAfter
import org.scalatest.Informing

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.concurrent.duration.DurationInt

object NativeReaderSpec {
  val isWin: Boolean = System.getProperty("os.name", "").toLowerCase.startsWith("win")
  def newServerSocket(socketName: String): ServerSocket = if (isWin)
    new Win32NamedPipeServerSocket(socketName, false, Win32SecurityLevel.LOGON_DACL)
  else new UnixDomainServerSocket(socketName, false)

  def newClientSocket(socketName: String): Socket = if (isWin)
    new Win32NamedPipeSocket(socketName, false)
  else new UnixDomainSocket(socketName, false)

  def unixSocketToNativeToArrow(
    nativeEvaluator: NativeEvaluator,
    inputList: List[String]
  ): String = {
    val res = nativeEvaluator
      .forCode("""#include "unix-read.cpp"""")
    println("Compiled.")
    val socketName =
      if (NativeReaderSpec.isWin) "\\\\.\\pipe\\tpipe"
      else s"/tmp/test-sock-${scala.util.Random.nextInt()}"
    val serverSocket = newServerSocket(socketName)
    val serverRespond = IO
      .blocking {
        println("Waiting for a connection...")
        val sockie = serverSocket.accept()
        println(s"Got a client connection! ${sockie}")
        try {
          val byteBuffer = ByteBuffer.allocate(4)
          inputList.foreach { str =>
            byteBuffer.position(0)
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            byteBuffer.putInt(str.length)
            byteBuffer.position(0)
            println("Writing to client...")
            sockie.getOutputStream.write(byteBuffer.array())
            sockie.getOutputStream.write(str.getBytes())
            sockie.getOutputStream.flush()
          }
          byteBuffer.position(0)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
          byteBuffer.putInt(0)
          byteBuffer.position(0)
          sockie.getOutputStream.write(byteBuffer.array())
          sockie.getOutputStream.close()
          println("Ended writing to client.")
        } finally sockie.close()
      }
      .timeout(15.seconds)

    val (x, y) = serverRespond.background.allocated.unsafeRunSync()

    println("HERE got here..")

    try {
      val allocator = new RootAllocator(Integer.MAX_VALUE)
      val vcv = new VarCharVector("test", allocator)
      try {
        println("Calling...")
        res
          .callFunction(
            "read_fully_2",
            List(Some(StringWrapper(socketName)), None),
            List(None, Some(VarCharVectorWrapper(vcv)))
          )
        println("ENded calling...")
        new String(vcv.get(0))
      } finally {
        vcv.close()
        serverSocket.close()
      }
    } finally {
      y.unsafeRunSync()
    }
  }

  def dataISunixSocketToNativeToArrow(
    nativeEvaluator: NativeEvaluator,
    inputStream: InputStream,
    bufSize: Int
  ): String = {
    val res = nativeEvaluator
      .forCode("""#include "unix-read.cpp"""")
    println("Compiled.")
    val socketName =
      if (NativeReaderSpec.isWin) "\\\\.\\pipe\\tpipe"
      else s"/tmp/test-sock-${scala.util.Random.nextInt()}"
    val serverSocket = newServerSocket(socketName)
    val serverRespond = IO
      .blocking {
        println("Waiting for a connection...")
        val sockie = serverSocket.accept()
        println(s"Got a client connection! ${sockie}")
        try {
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
            sockie.getOutputStream.write(byteBuffer.array())
            sockie.getOutputStream.write(buf, 0, bytesRead)
            sockie.getOutputStream.flush()
            bytesRead = inputStream.read(buf)
            if (bytesRead < 1) {
              hasMore = false
            }
          }

          byteBuffer.position(0)
          byteBuffer.putInt(0)
          byteBuffer.position(0)
          println("Writing to client...")
          sockie.getOutputStream.write(byteBuffer.array())
          sockie.getOutputStream.flush()
          sockie.getOutputStream.close()
          println("Ended writing to client.")
        } finally sockie.close()
      }
      .timeout(15.seconds)

    val (x, y) = serverRespond.background.allocated.unsafeRunSync()

    println("HERE got here..")

    try {
      val allocator = new RootAllocator(Integer.MAX_VALUE)
      val vcv = new VarCharVector("test", allocator)
      try {
        println("Calling...")
        res
          .callFunction(
            "read_fully_2",
            List(Some(StringWrapper(socketName)), None),
            List(None, Some(VarCharVectorWrapper(vcv)))
          )
        println("ENded calling...")
        new String(vcv.get(0))
      } finally {
        vcv.close()
        serverSocket.close()
      }
    } finally {
      y.unsafeRunSync()
    }
  }
}

final class NativeReaderSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with Informing
  with SparkAdditions {
  "We can put stuff into a UNIX socket, and it will give us data back" ignore {
    val socketName =
      if (NativeReaderSpec.isWin) "\\\\.\\pipe\\ipcsockettest"
      else s"/tmp/test-sock-${scala.util.Random.nextInt()}"
    val serverSocket = newServerSocket(socketName)
    val serverRespond = IO.delay {
      val sockie = serverSocket.accept()
      try {
        val num = sockie.getInputStream.read()
        sockie.getOutputStream.write(Array[Byte](1, num.toByte, 2))
        sockie.getOutputStream.flush()
        sockie.getOutputStream.close()
      } finally sockie.close()
    }

    val (x, y) = serverRespond.background.allocated.unsafeRunSync()
    try {
      val clientConnect = newClientSocket(socketName)
      clientConnect.getOutputStream.write(9)
      clientConnect.getOutputStream.flush()
      val result = List(
        clientConnect.getInputStream.read(),
        clientConnect.getInputStream.read(),
        clientConnect.getInputStream.read()
      )
      clientConnect.close()
      assert(result == List(1, 9, 2))
    } finally {
      y.unsafeRunSync()
    }
  }

  "We can transfer Hadoop data to the native app" ignore withSparkSession2(identity) {

    /** Not yet implemented properly - this tests fails */

    sparkSession =>
      val listOfPairs = sparkSession.sparkContext
        .wholeNativeTextFiles(samplePartedCsv)
        .collect()
        .toList
        .map { case (name, ver) =>
          name -> new String(ver.getRawData)
        }

      expect(listOfPairs.size == 3, listOfPairs.exists(_._2.contains("5.0,4.0,3.0")))
  }

  "We can read-write to a native app" ignore {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
//    WithTestAllocator { allocator =>
    val vcv = new VarCharVector("test", allocator)
    val inputSock = new VarCharVector("inputSock", allocator)
    inputSock.setValueCount(1)
    inputSock.setSafe(0, new Text("ABC"))
    try {
      CNativeEvaluator
        .forCode("""#include "unix-read.cpp"""")
        .callFunction(
          "read_fully",
          List(Some(VarCharVectorWrapper(inputSock)), None),
          List(None, Some(VarCharVectorWrapper(vcv)))
        )
      assert(new String(vcv.get(0)) == "ABC")
    } finally vcv.close()
//    }
  }

  "We can read-write with a unix socket" ignore {
    val inputList = List("ABC", "DEF", "GHQEWE")
    if (!scala.util.Properties.isWin) {
      val expectedString = inputList.mkString
      assert(unixSocketToNativeToArrow(CNativeEvaluator, inputList) == expectedString)
    }
  }

  "We can read-write with a unix socket from an input stream" in {
    val inputList = List("ABC", "DEF", "GHQEWE123")
    val inputStream = new ByteArrayInputStream(inputList.mkString.getBytes())
    if (!scala.util.Properties.isWin) {
      val expectedString = inputList.mkString
      assert(dataISunixSocketToNativeToArrow(CNativeEvaluator, inputStream, 4) == expectedString)
    }
  }

}
