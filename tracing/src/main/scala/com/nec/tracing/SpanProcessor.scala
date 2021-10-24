package com.nec.tracing

import com.nec.tracing.SpanProcessor.SpanFound

import java.time.Duration

object SpanProcessor {
  def start: SpanProcessor = SpanProcessor(awaitingEnd = Nil, emitted = None)

  def analyzeLines(lines: List[String]): List[String] = {
    val spansFound = lines.iterator
      .flatMap(line => TracingRecord.parse(line))
      .scanLeft(SpanProcessor.start)(_.process(_))
      .flatMap(_.emitted)
      .toList

    val partitionSpans = spansFound
      .flatMap(span => span.start.partId.map(partId => span.start.positionName -> span.duration))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .toMap
      .toList
      .sortBy(_._2.max)
      .reverse
      .map { case (name, durations) =>
        List(
          s"Count: ${durations.size}",
          s"Median: ${durations.median}",
          s"Max: ${durations.max}"
        ) -> name
      }
      .tabulate

    val partitionExecutorSpans = spansFound
      .filter(_.start.executorId.nonEmpty)
      .flatMap(span =>
        span.start.partId.map(partId =>
          (span.start.positionName, span.start.executorId) -> span.duration
        )
      )
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .toList
      .sortBy { case ((n, exid), cts) => (exid, cts.max) }
      .reverse
      .map { case ((pn, exId), durations) =>
        List(
          s"Count: ${durations.size}",
          s"Median: ${durations.median}",
          s"Max: ${durations.max}"
        ) -> s"${exId.getOrElse("")} | ${pn}"
      }
      .tabulate

    val nonPartition = spansFound
      .filterNot(_.inPartition)
      .sortBy(_.duration)
      .reverse
      .map(span => s"[${span.duration}] ${span.start.positionName}")

    List(nonPartition, List("--"), partitionSpans, List("--"), partitionExecutorSpans).flatten
  }

  implicit class RichListStr(ls: List[(List[String], String)]) {
    def tabulate: List[String] = {
      val colWidths = ls.map(_._1).transpose.map(_.map(_.length).max)

      ls.map { case (cols, last) =>
        cols
          .zip(colWidths)
          .zipWithIndex
          .map { case ((col, padding), idx) =>
            val ws = if (colWidths.indices.lastOption.contains(idx)) s"$col" else s"$col,"
            val padLen = if (colWidths.indices.lastOption.contains(idx)) padding else padding + 1
            val t = ws.padTo(padLen, ' ')
            if (idx == 0) t else s" $t"
          }
          .mkString("[", "", "]") + " " + last
      }
    }
  }

  final case class SpanFound(start: TracingRecord, end: TracingRecord) {
    def inPartition: Boolean = start.partId.nonEmpty
    def duration: Duration = Duration.between(start.instant, end.instant)
  }

  implicit class RichL(l: List[Duration]) {
    def median: Duration = {
      if (l.length == 1)
        l(0)
      else if (l.length == 2)
        l(0).plus(l(1)).dividedBy(2)
      if (l.length % 2 == 0) {
        l(l.length / 2 - 1).plus(l(l.length / 2)).dividedBy(2)
      } else l(l.length / 2)
    }
    def total: Duration = {
      l.reduce(_.plus(_))
    }
  }
}

final case class SpanProcessor(awaitingEnd: List[TracingRecord], emitted: Option[SpanFound]) {
  def process(event: TracingRecord): SpanProcessor = {
    if (event.position.startsWith("S:")) copy(awaitingEnd = event :: awaitingEnd, emitted = None)
    else if (event.position.startsWith("E:")) {
      awaitingEnd
        .filter(_.currentContext == event.currentContext)
        .find(tr => event.positionName == tr.positionName) match {
        case None =>
          sys.error(
            s"Could not find a corresponding start event for ${event}; awaiting from ${awaitingEnd}"
          )
        case Some(startEvent) =>
          copy(
            awaitingEnd = awaitingEnd.filterNot(_ == startEvent),
            emitted = Some(SpanFound(startEvent, event))
          )
      }
    } else copy(emitted = None)
  }
}
