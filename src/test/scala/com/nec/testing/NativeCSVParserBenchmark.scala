package com.nec.testing
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.CsvParse
import com.nec.aurora.Aurora
import com.nec.cmake.CNativeEvaluator
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.testing.NativeCSVParserBenchmark.ParserTestState
import com.nec.testing.NativeCSVParserBenchmark.SimpleTestType
import com.nec.testing.Testing.TestingTarget
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.compile_cpp
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

import java.nio.ByteBuffer
import java.nio.file.Files

object NativeCSVParserBenchmark {
  sealed trait SimpleTestType {
    def testingTarget: TestingTarget
  }
  object SimpleTestType {
    case object CBased extends SimpleTestType {
      override def toString: String = "_CMake"
      override def testingTarget: TestingTarget = TestingTarget.CMake
    }
    case object VEBased extends SimpleTestType {
      override def toString: String = "_VE"
      override def testingTarget: TestingTarget = TestingTarget.VectorEngine
    }
  }
  trait ParserTestState {
    def originalArray: Array[Double]
    def interface: ArrowNativeInterfaceNumeric
    def bufferAllocator: BufferAllocator

    val alloc = new RootAllocator(Integer.MAX_VALUE)
    def close(): Unit
    def byteBuffer: ByteBuffer
    def totalSize: Int
  }
}

final case class NativeCSVParserBenchmark(simpleTestType: SimpleTestType) extends GenericTesting {
  override type State = ParserTestState
  override def benchmark(state: State): Unit = {
    val a = new Float8Vector("a", state.bufferAllocator)
    val b = new Float8Vector("b", state.bufferAllocator)
    val c = new Float8Vector("c", state.bufferAllocator)
    CsvParse.runOn(state.interface)(Left(state.byteBuffer -> state.totalSize), a, b, c)

    val randomRow = scala.util.Random.nextInt(state.originalArray.length / 3)
    val randomCol = scala.util.Random.nextInt(3)

    assert(
      state.originalArray(3 * randomRow + randomCol) == List(a, b, c)(randomCol).get(randomRow)
    )
  }
  override def cleanUp(state: State): Unit = {
    println(s"Cleaning up! ${state}")
  }
  override def testingTarget: Testing.TestingTarget = simpleTestType.testingTarget
  private val MEGABYTE = 1024 * 1024
  override def init(): State = {
    val minimum = 512 * MEGABYTE
    val arrItems = scala.collection.mutable.Buffer.empty[Double]
    val theBuffer = ByteBuffer.allocateDirect(minimum + 1024)
    theBuffer.put("a,b,c\n".getBytes())
    while (theBuffer.position() < minimum) {
      val newItems = List.fill(3)(scala.util.Random.nextDouble())
      arrItems ++= newItems
      theBuffer.put((newItems.mkString(",") + "\n").getBytes())
    }
    val inputArray = arrItems.toArray
    arrItems.clear()
    val madeTotalSize = theBuffer.position()
    assert(
      theBuffer.position() > minimum,
      s"Require data size to be big enough, had ${theBuffer.position()}, expected $minimum"
    )
    theBuffer.position(0)
    simpleTestType match {
      case SimpleTestType.CBased =>
        new ParserTestState {
          val bufferAllocator = new RootAllocator(Integer.MAX_VALUE)
          val interface = CNativeEvaluator.forCode(CsvParse.CsvParseCode)
          override def close(): Unit = ()
          override def byteBuffer: ByteBuffer = theBuffer
          override def originalArray: Array[Double] = inputArray
          override def totalSize: Int = madeTotalSize
        }
      case SimpleTestType.VEBased =>
        new ParserTestState {
          override def byteBuffer: ByteBuffer = theBuffer
          val bufferAllocator = new RootAllocator(Integer.MAX_VALUE)
          val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
          val soName = compile_cpp(
            buildDir = tmpBuildDir,
            config = VeKernelCompiler.VeCompilerConfig.testConfig,
            List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode).mkString(
              "\n\n"
            )
          ).toAbsolutePath.toString
          val proc = Aurora.veo_proc_create(0)
          val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)

          val lib = Aurora.veo_load_library(Aurora4SparkExecutorPlugin._veo_proc, soName)
          require(lib != 0, s"Expected lib != 0, got ${lib}")

          val interface =
            new VeArrowNativeInterfaceNumeric(proc, ctx, lib)

          def close(): Unit = {
            Aurora.veo_context_close(ctx)
            Aurora.veo_proc_destroy(proc)
          }

          override def originalArray: Array[Double] = inputArray
          override def totalSize: Int = madeTotalSize
        }
    }
  }
}
