package com.nec.util

object BatchAmplifier {
  def amplify[T](i: Iterator[T])(limit: Int, f: T => Int): Iterator[Vector[T]] = {
    val buffer = scala.collection.mutable.Buffer.empty[T]
    var accumulated: Int = 0
    new scala.Iterator[Vector[T]] {
      override def hasNext: Boolean = {
        var hasNext = i.hasNext
        if (!hasNext && buffer.nonEmpty) {
          true
        } else {
          while (accumulated < limit && hasNext) {
            val item = i.next()
            buffer.append(item)
            accumulated += f(item)
            hasNext = i.hasNext
          }
          accumulated = 0
          buffer.nonEmpty
        }
      }
      override def next(): Vector[T] = {
        if (buffer.isEmpty) throw new NoSuchElementException(s"Cannot produce an element")
        else {
          val res = buffer.toVector
          buffer.clear()
          res
        }
      }
    }
  }

  object Implicits {
    final implicit class RichIterator[T](i: Iterator[T]) {
      def amplify(limit: Int, f: T => Int): Iterator[Vector[T]] =
        BatchAmplifier.amplify(i)(limit, f)
    }
  }
}
