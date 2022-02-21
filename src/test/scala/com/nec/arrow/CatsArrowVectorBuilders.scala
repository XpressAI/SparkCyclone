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
package com.nec.arrow

import cats.effect.{IO, Ref, Resource}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector, IntVector, VarCharVector}

final case class CatsArrowVectorBuilders(vectorCount: Ref[IO, Int])(implicit
  bufferAllocator: BufferAllocator
) {

  private def makeName: Resource[IO, String] =
    Resource.eval(vectorCount.updateAndGet(_ + 1)).map(int => s"field_${int}")

  import cats.implicits._
  def stringVector(stringBatch: Seq[String]): Resource[IO, VarCharVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new VarCharVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              stringBatch.view.zipWithIndex.foreach { case (str, idx) =>
                vcv.setSafe(idx, str.getBytes("UTF-8"), 0, str.length)
              }
              vcv.setValueCount(stringBatch.length)
            }
          }
      )(res => IO.delay(res.close()))
    )
  def doubleVector(doubleBatch: Seq[Double]): Resource[IO, Float8Vector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new Float8Vector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              doubleBatch.view.zipWithIndex.foreach { case (str, idx) => vcv.setSafe(idx, str) }
              vcv.setValueCount(doubleBatch.length)
              vcv
            }
          }
      )(res => IO.delay(res.close()))
    )

  def intVector(intBatch: Seq[Int]): Resource[IO, IntVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new IntVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              intBatch.view.zipWithIndex.foreach { case (str, idx) => vcv.setSafe(idx, str) }
              vcv.setValueCount(intBatch.length)
              vcv
            }
          }
      )(res => IO.delay(res.close()))
    )

  def optionalIntVector(intBatch: Seq[Option[Int]]): Resource[IO, IntVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new IntVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              intBatch.view.zipWithIndex.foreach {
                case (None, idx)      => vcv.setNull(idx)
                case (Some(str), idx) => vcv.setSafe(idx, str)
              }
              vcv.setValueCount(intBatch.length)
              vcv
            }
          }
      )(res => IO.delay(res.close()))
    )

  def longVector(longBatch: Seq[Long]): Resource[IO, BigIntVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new BigIntVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              longBatch.view.zipWithIndex.foreach { case (str, idx) => vcv.setSafe(idx, str) }
              vcv.setValueCount(longBatch.length)
              vcv
            }
          }
      )(res => IO.delay(res.close()))
    )
}
