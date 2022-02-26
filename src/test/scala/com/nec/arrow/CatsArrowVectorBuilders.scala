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
import org.apache.arrow.vector.{
  BigIntVector,
  DateDayVector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  VarCharVector
}

import java.time.{Duration, Instant, LocalDate, ZoneId}
import org.apache.arrow.vector.{
  BigIntVector,
  DateDayVector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  VarCharVector
}

import java.time.{Duration, LocalDate, ZoneId}
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

  def shortVector(shortBatch: Seq[Short]): Resource[IO, SmallIntVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new SmallIntVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              shortBatch.view.zipWithIndex.foreach { case (short, idx) => vcv.setSafe(idx, short) }
              vcv.setValueCount(shortBatch.length)
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

  def shortVector(shortBatch: Seq[Short]): Resource[IO, SmallIntVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new SmallIntVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              shortBatch.view.zipWithIndex.foreach { case (short, idx) => vcv.setSafe(idx, short) }
              vcv.setValueCount(shortBatch.length)
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

  def dateVector(localDates: Seq[LocalDate]): Resource[IO, DateDayVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new DateDayVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              localDates.view.zipWithIndex.foreach { case (str, idx) =>
                val duration = Duration.between(
                  LocalDate.parse("1970-01-01").atStartOfDay(ZoneId.of("UTC")),
                  str.atStartOfDay(ZoneId.of("UTC"))
                )
                vcv.setSafe(idx, duration.toDays.toInt)
              }
              vcv.setValueCount(localDates.length)
              vcv
            }
          }
      )(res => IO.delay(res.close()))
    )

  def timestampVector(timestamps: Seq[Instant]): Resource[IO, BigIntVector] =
    makeName.flatMap(name =>
      Resource.make(
        IO.delay(new BigIntVector(name, bufferAllocator))
          .flatTap { vcv =>
            IO.delay {
              timestamps.view.zipWithIndex.foreach { case (instant, idx) =>
                vcv.setSafe(idx, instant.toEpochMilli)
              }
              vcv.setValueCount(timestamps.length)
              vcv
            }
          }
      )(res => IO.delay(res.close()))
    )

}
