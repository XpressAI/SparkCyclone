package io.sparkcyclone.cache

import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.data.InputSamples
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.vectorengine.WithVeProcess
import scala.util.Random
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeColBatchesCacheUnitSpec extends AnyWordSpec with WithVeProcess {
  val cache = new VeColBatchesCache

  "VeColBatchesCache" should {
    "correctly register VeColBatches for cache tracking and free VeColBatches on cleanup" in {
      val sizeA = Random.nextInt(50) + 10
      val a1 = InputSamples.seqOpt[Int](sizeA).toBytePointerColVector("_")
      val a2 = InputSamples.seqOpt[Double](sizeA).toBytePointerColVector("_")
      val a3 = InputSamples.seqOpt[String](sizeA).toBytePointerColVector("_")

      val sizeB = Random.nextInt(50) + 10
      val b1 = InputSamples.seqOpt[Int](sizeB).toBytePointerColVector("_")
      val b2 = InputSamples.seqOpt[Double](sizeB).toBytePointerColVector("_")

      val sizeC = Random.nextInt(50) + 10
      val c1 = InputSamples.seqOpt[Double](sizeC).toBytePointerColVector("_")
      val c2 = InputSamples.seqOpt[String](sizeC).toBytePointerColVector("_")

      val batch1 = VeColBatch(Seq(a1, a2, a3).map(_.toVeColVector))
      val batch2 = VeColBatch(Seq(b1, b2).map(_.toVeColVector))
      val batch3 = VeColBatch(Seq(c1, c2).map(_.toVeColVector))

      batch1.columns.map(_.isOpen).toSet should be (Set(true))
      batch2.columns.map(_.isOpen).toSet should be (Set(true))
      batch3.columns.map(_.isOpen).toSet should be (Set(true))

      cache.register("batch1", batch1)
      cache.register("batch2", batch2)
      cache.batches.size should be (2)

      cache.cleanupIfNotCached(batch2)
      cache.cleanupIfNotCached(batch3)
      cache.batches.size should be (2)
      batch1.columns.map(_.isOpen).toSet should be (Set(true))
      batch2.columns.map(_.isOpen).toSet should be (Set(true))
      batch3.columns.map(_.isOpen).toSet should be (Set(false))

      cache.cleanup
      cache.batches.size should be (0)
      batch1.columns.map(_.isOpen).toSet should be (Set(false))
      batch2.columns.map(_.isOpen).toSet should be (Set(false))
      batch3.columns.map(_.isOpen).toSet should be (Set(false))
    }
  }
}
