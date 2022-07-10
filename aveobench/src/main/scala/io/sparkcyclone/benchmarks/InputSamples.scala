package io.sparkcyclone.benchmarks

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.vector._

import scala.reflect.ClassTag
import scala.util.Random

object InputSamples {
  def seqOpt[T: ClassTag](size: Int): Seq[Option[T]] = {
    seq[T](size).map(x => if (Math.random < 0.5) Some(x) else None)
  }

  def seqOpt[T: ClassTag]: Seq[Option[T]] = {
    seqOpt[T](Random.nextInt(100))
  }

  def seq[T: ClassTag](size: Int): Seq[T] = {
    val klass = implicitly[ClassTag[T]].runtimeClass

    if (klass == classOf[Int]) {
      0.until(size).map(_ => Random.nextInt(10000)).asInstanceOf[Seq[T]]

    } else if (klass == classOf[Short]) {
      0.until(size).map(_ => Random.nextInt(10000).toShort).asInstanceOf[Seq[T]]

    } else if (klass == classOf[Long]) {
      0.until(size).map(_ => Random.nextLong).asInstanceOf[Seq[T]]

    } else if (klass == classOf[Float]) {
      0.until(size).map(_ => Random.nextFloat * 1000).asInstanceOf[Seq[T]]

    } else if (klass == classOf[Double]) {
      0.until(size).map(_ => Random.nextDouble * 100000).asInstanceOf[Seq[T]]

    } else if (klass == classOf[String]) {
      0.until(size).map(_ => Random.nextString(30)).asInstanceOf[Seq[T]]

    } else {
      throw new NotImplementedError(s"Type not supported: ${klass}")
    }
  }

  def bpcv(typ: String, ncolumns: Int, size: Int)(implicit source: VeColVectorSource): Seq[BytePointerColVector] = {
    typ match {
      case "Short"  => 0.until(ncolumns).map(_ => seqOpt[Short](size).toBytePointerColVector("_"))
      case "Double" => 0.until(ncolumns).map(_ => seqOpt[Double](size).toBytePointerColVector("_"))
      case "String" => 0.until(ncolumns).map(_ => seqOpt[String](size).toBytePointerColVector("_"))
      case _        => 0.until(ncolumns).map(_ => seqOpt[Int](size).toBytePointerColVector("_"))
    }
  }
}
