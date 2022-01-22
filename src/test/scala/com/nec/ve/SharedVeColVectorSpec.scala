package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.arrow.WithTestAllocator
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.SharedVectorEngineMemory
import com.nec.ve.colvector.VeColBatch.{VeColVector, VeColVectorSource}
import org.bytedeco.javacpp.Pointer
import org.bytedeco.veoffload.global.veo
import org.scalatest.freespec.AnyFreeSpec
import sun.nio.ch.DirectBuffer

import java.nio.ByteBuffer
import java.nio.file.Files

final class SharedVeColVectorSpec extends AnyFreeSpec with WithVeProcess {
  "Memory can be passed in and out" in {
    val tempFile = Files.createTempFile("test", "test")
    val mem = SharedVectorEngineMemory.make(tempFile.toAbsolutePath.toString, 0, 100)
    val dir = ByteBuffer.allocateDirect(100)
    val lst = List[Byte](1, 2, 9)
    dir.put(lst.toArray)
    dir.position(0)
    val newOne = mem.copy(dir.asInstanceOf[DirectBuffer], 3)
    val bb = mem.read(newOne, 3)
    val tgt = Array.fill[Byte](3)(-1)
    bb.get(tgt)
    val recives = tgt.toList
    assert(recives == lst)
  }
  "We can transfer from one VE process to another" in {
    val source: VeColVectorSource = VeColVectorSource(s"VE Tests")
    val proc1 = veo.veo_proc_create(-1)
    assert(proc1.getPointer[Pointer].address() > 0)
    val proc2 = veo.veo_proc_create(-1)
    assert(proc2.getPointer[Pointer].address() > 0)
    val veProcess1: VeProcess = VeProcess.WrappingVeo(proc1, source, VeProcessMetrics.NoOp)
    val veProcess2: VeProcess = VeProcess.WrappingVeo(proc2, source, VeProcessMetrics.NoOp)

    val tmpPath = "/dev/shm/x"
    val sharedEngine1 = SharedVectorEngineMemory.make(tmpPath, 0, 10000)
    val sharedEngine2 = SharedVectorEngineMemory.make(tmpPath, 900, 10000)

    import OriginalCallingContext.Automatic._
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector =
          VeColVector.fromArrowVector(f8v)(veProcess1, source, implicitly[OriginalCallingContext])
        val sharedVec =
          SharedVectorEngineMemory.SharedColVector
            .fromVeColVector(colVec)(veProcess1, sharedEngine1, implicitly[OriginalCallingContext])
        val secondVec =
          sharedVec.toVeColVector()(
            veProcess2,
            sharedEngine2,
            implicitly[OriginalCallingContext],
            source
          )
        val arrowVec = secondVec.toArrowVector()(veProcess2, alloc)
        try {
          try {
            colVec.free()(veProcess1, source, implicitly[OriginalCallingContext])
            expect(arrowVec.toString == f8v.toString)
          } finally arrowVec.close()
        } finally {
          sharedEngine1.close()
          sharedEngine2.close()
        }
      }
    }
  }
}
