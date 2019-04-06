package org.mvrs.dspa.functions

import java.util.Random

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.mvrs.dspa.functions.ScaledReplayFunction.toReplayTime
import org.mvrs.dspa.functions.TextFileSourceFunction._
import org.mvrs.dspa.utils

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

class TextFileSourceFunction[OUT](filePath: String,
                                  skipFirstLine: Boolean,
                                  parse: String => OUT,
                                  extractEventTime: OUT => Long,
                                  speedupFactor: Double,
                                  maximumDelayMillis: Long,
                                  delay: OUT => Long) extends RichSourceFunction[OUT] {

  private lazy val replayStartTime: Long = System.currentTimeMillis
  private val queue = mutable.PriorityQueue.empty[DelayedEvent[OUT]](Ordering.by((_: DelayedEvent[OUT]).delayedEventTime).reverse)

  private var firstEventTime = Long.MinValue
  private var maximumEventTime = Long.MinValue
  private var source: BufferedSource = _

  @volatile
  private var isRunning = true

  def this(filePath: String,
           skipFirstLine: Boolean,
           parse: String => OUT,
           extractEventTime: OUT => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Long = 0) =
    this(filePath, skipFirstLine, parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override def open(parameters: Configuration): Unit = {
    source = Source.fromFile(filePath)

    // consider using org.apache.flink.api.java.io.CsvInputFormat
    // var format = new org.apache.flink.api.java.io.TupleCsvInputFormat[OUT](filePath)
    // then ... format.openInputFormat() ... format.nextRecord()
    // this would support reading in parallel from splits, which we don't want here
  }

  override def close(): Unit = source.close()

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    for (line <- getLines.takeWhile(_ => isRunning)) {
      val event = parse(line)
      val eventTime = extractEventTime(event)

      assert(eventTime > Long.MinValue, s"invalid event time: $eventTime ($event)")
      assert(eventTime >= maximumEventTime, s"event time $eventTime < maximum event time $maximumEventTime")

      maximumEventTime = eventTime

      val delayMillis = delay(event)
      assert(delayMillis <= maximumDelayMillis, s"delay $delayMillis exceeds maximum $maximumDelayMillis")

      queue += DelayedEvent(eventTime + delayMillis, event)

      if (firstEventTime == Long.MinValue) firstEventTime = eventTime

      processQueue(ctx)
    }

    processQueue(ctx, flush = true)
  }

  // TODO abstract over source?
  private def getLines = if (skipFirstLine) source.getLines().drop(1) else source.getLines()

  private def processQueue(ctx: SourceFunction.SourceContext[OUT], flush: Boolean = false): Unit = {
    while (queue.nonEmpty && (flush || queue.head.delayedEventTime <= maximumEventTime)) {
      val head = queue.dequeue()

      val replayTime = toReplayTime(replayStartTime, firstEventTime, head.delayedEventTime, speedupFactor)
      val waitTime = replayTime - System.currentTimeMillis()
      println(s"replay time: $replayTime - delayed event time: ${head.delayedEventTime} - wait time: $waitTime - event: ${head.event}")
      Thread.sleep(if (waitTime > 0) waitTime else 0)

      // TODO emit watermark also?
      ctx.collectWithTimestamp(head.event, head.delayedEventTime)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

object TextFileSourceFunction {
  private val rand = new Random(137)

  def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long, speedupFactor: Double): Long = {
    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }

  private case class DelayedEvent[I](delayedEventTime: Long, event: I)
}
