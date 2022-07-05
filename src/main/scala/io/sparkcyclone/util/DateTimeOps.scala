package io.sparkcyclone.util

import scala.math.Ordered
import java.time.Instant

object DateTimeOps {
  object ExtendedInstant {
    def fromFrovedisDateTime(x: Long): Instant = {
      Instant.ofEpochSecond((x / 1000000000).toLong, (x % 1000000000).toLong)
    }
  }

  implicit class ExtendedInstant(val x: Instant) extends Ordered[ExtendedInstant] {
    def toFrovedisDateTime: Long = {
      x.getEpochSecond * 1000000000 + x.getNano.toLong
    }

    def compare(other: ExtendedInstant): Int = {
      x.compareTo(other.x)
    }
  }
}
