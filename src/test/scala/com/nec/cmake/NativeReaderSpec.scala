package com.nec.cmake

import cats.effect.{ContextShift, IO, Resource}
import com.eed3si9n.expecty.Expecty.expect
import com.google.common.io.ByteStreams
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.StringWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.VarCharVectorWrapper
import com.nec.cmake.NativeReaderSpec.dataISunixSocketToNativeToArrow
import com.nec.cmake.NativeReaderSpec.newClientSocket
import com.nec.cmake.NativeReaderSpec.newServerSocket
import com.nec.cmake.NativeReaderSpec.unixSocketToNativeToArrow
import com.nec.cmake.ReadFullCSVSpec.samplePartedCsv
import com.nec.native.IpcTransfer.transferIPC
import com.nec.native.NativeEvaluator
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.NativeCsvExec.maybeDecodePds
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.util.Text

import org.apache.spark.sql.SparkSession
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

import scala.concurrent.ExecutionContext.global

import com.nec.spark.planning.{SerializableConfiguration, SparkPortingUtils}

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
    val inputStream = new ByteArrayInputStream(inputList.mkString.getBytes())
    val bufSize = 4
    val (socketName, serverSocket) = transferIPC(inputStream, bufSize)
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val vcv = new VarCharVector("test", allocator)
    try {
      res
        .callFunction(
          "read_fully_2",
          List(Some(StringWrapper(socketName)), None),
          List(None, Some(VarCharVectorWrapper(vcv)))
        )
      new String(vcv.get(0))
    } finally {
      vcv.close()
      serverSocket.close()
    }
  }

  def dataISunixSocketToNativeToArrow(
    res: ArrowNativeInterfaceNumeric,
    inputStream: InputStream,
    bufSize: Int
  ): String = {
    val (socketName, serverSocket) = transferIPC(inputStream, bufSize)

    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val vcv = new VarCharVector("test", allocator)
    try {
      res
        .callFunction(
          "read_fully_2",
          List(Some(StringWrapper(socketName)), None),
          List(None, Some(VarCharVectorWrapper(vcv)))
        )
      new String(vcv.get(0))
    } finally {
      vcv.close()
      serverSocket.close()
    }
  }
}

final class NativeReaderSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with Informing
  with SparkAdditions {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  "We can put stuff into a UNIX socket, and it will give us data back" in {
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
    val (x, y) =  Resource.make(serverRespond.start)(_.cancel).map(_.join)
      .allocated
      .unsafeRunSync()

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

  "We can transfer Hadoop data to the native app" in withSparkSession2(identity) {
    sparkSession =>
      val hadoopConf = new SerializableConfiguration(sparkSession.sparkContext.hadoopConfiguration)
      val listOfPairs =
        sparkSession.sparkContext
          .binaryFiles(samplePartedCsv)
          .collect()
          .toList
          .map { case (name, pds) =>
            name -> new String(
              ByteStreams.toByteArray(
                maybeDecodePds(
                  name,
                  hadoopConf,
                  pds
                )
              )
            )
          }

      expect(listOfPairs.size == 3, listOfPairs.exists(_._2.contains("5.0,4.0,3.0")))
  }

  "We can read-write to a native app" ignore {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
//    WithTestAllocator { allocator =>
    val vcv = new VarCharVector("test", allocator)
    val inputSock = new VarCharVector("inputSock", allocator)
    inputSock.setValueCount(1)
    inputSock.setSafe(0, new Text("ABC").getBytes)
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
    if (!scala.util.Properties.isWin) {
      val inputStream = new ByteArrayInputStream(inputList.mkString.getBytes())
      val expectedString = inputList.mkString
      assert(
        dataISunixSocketToNativeToArrow(
          CNativeEvaluator
            .forCode("""#include "unix-read.cpp""""),
          inputStream,
          4
        ) == expectedString
      )
    }
  }

}
