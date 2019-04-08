package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.functions.ScaledReplayFunction.toReplayTime
import org.mvrs.dspa.utils

import scala.collection.mutable
import scala.util.Random

abstract class ReplayedSourceFunction[IN, OUT](parse: IN => OUT,
                                               extractEventTime: OUT => Long,
                                               speedupFactor: Double,
                                               maximumDelayMillis: Long,
                                               delay: OUT => Long) extends RichSourceFunction[OUT] {

  private lazy val replayStartTime: Long = System.currentTimeMillis
  private val queue = mutable.PriorityQueue.empty[DelayedEvent[OUT]](Ordering.by((_: DelayedEvent[OUT]).delayedEventTime).reverse)

  private var firstEventTime = Long.MinValue
  private var maximumEventTime = Long.MinValue

  @volatile private var isRunning = true

  protected def this(parse: IN => OUT,
                     extractEventTime: OUT => Long,
                     speedupFactor: Double = 0,
                     maximumDelayMilliseconds: Long = 0) =
    this(parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    for (input <- inputIterator.takeWhile(_ => isRunning)) {
      val event = parse(input)
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

  protected def inputIterator: Iterator[IN]

  private def processQueue(ctx: SourceFunction.SourceContext[OUT], flush: Boolean = false): Unit = {
    while (queue.nonEmpty && (flush || queue.head.delayedEventTime <= maximumEventTime)) {
      val head = queue.dequeue()

      val replayTime = toReplayTime(replayStartTime, firstEventTime, head.delayedEventTime, speedupFactor)
      val waitTime = replayTime - System.currentTimeMillis()
      println(s"replay time: $replayTime - delayed event time: ${head.delayedEventTime} - wait time: $waitTime - event: ${head.event}")
      Thread.sleep(if (waitTime > 0) waitTime else 0)

      // TODO emit watermark also?
      ctx.collectWithTimestamp(head.event, extractEventTime(head.event))
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

object ReplayedSourceFunction {
  private[functions] val rand = new Random(137)

  def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long, speedupFactor: Double): Long = {
    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }

  private case class DelayedEvent[I](delayedEventTime: Long, event: I)

}