package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import scala.reflect.ClassTag
import scala.util.Random

object InputSamples {
  def seqOpt[T: ClassTag](size: Int): Seq[Option[T]] = {
    seq[T](size).map(x => if (Math.random < 0.5) Some(x) else None)
  }

  def seqOpt[T: ClassTag]: Seq[Option[T]] = {
    seqOpt[T](Random.nextInt(100))
  }

  def array[T: ClassTag](size: Int): Array[T] = {
    seq[T](size).toArray
  }

  def array[T: ClassTag]: Array[T] = {
    seq[T](Random.nextInt(100)).toArray
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
      0.until(size).map(_ => Random.nextString(Random.nextInt(30) + 1)).asInstanceOf[Seq[T]]

    } else if (klass == classOf[Byte]) {
      val bytes = Array.ofDim[Byte](size.abs)
      Random.nextBytes(bytes)
      bytes.toSeq.asInstanceOf[Seq[T]]

    } else {
      throw new NotImplementedError(s"Type not supported: ${klass}")
    }
  }
}
