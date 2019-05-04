package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.watermark.Watermark
import org.mvrs.dspa.functions.EventScheduler._
import org.slf4j.LoggerFactory

import scala.collection.mutable

class EventScheduler[OUT](speedupFactor: Double,
                          watermarkIntervalMillis: Long,
                          maximumDelayMillis: Long,
                          delay: OUT => Long,
                          expectOrdered: Boolean = true) {
  require(watermarkIntervalMillis > 0, s"invalid watermark interval: $watermarkIntervalMillis")
  require(speedupFactor >= 0, s"invalid speedup factor: $speedupFactor")

  private lazy val replayStartTime: Long = System.currentTimeMillis
  private val queue = mutable.PriorityQueue.empty[(Long, Either[(OUT, Long), Watermark])](Ordering.by((_: (Long, Either[(OUT, Long), Watermark]))._1).reverse)

  private var firstEventTime = Long.MinValue
  private var maximumEventTime: Long = Long.MinValue

  private val LOG = LoggerFactory.getLogger(classOf[EventScheduler[OUT]])

  def schedule(event: OUT, eventTime: Long): Unit = {
    // TODO this can be violated when reading from multiple kafka topics
    if (expectOrdered) assert(eventTime >= maximumEventTime, s"event time $eventTime < maximum $maximumEventTime")

    maximumEventTime = math.max(eventTime, maximumEventTime)

    val delayMillis = delay(event) // in processing time, but at the rate of the event time (i.e. subject to speedup)
    assert(delayMillis <= maximumDelayMillis, s"delay $delayMillis exceeds maximum $maximumDelayMillis")

    if (firstEventTime == Long.MinValue) firstEventTime = eventTime

    if (queue.isEmpty) scheduleWatermark(eventTime)

    queue += ((eventTime + delayMillis, Left((event, eventTime)))) // schedule the event
  }

  def processPending(emitEvent: (OUT, Long) => Unit,
                     emitWatermark: Watermark => Unit,
                     wait: Long => Unit,
                     isCancelled: () => Boolean,
                     flush: Boolean): Option[Long] = {
    // dequeue only if the next delayed event time is within maximum event time, to make sure that no later events can
    // schedule before the next delayed event - or when flushing the queue at end of input
    while (queue.nonEmpty && (flush || queue.head._1 <= maximumEventTime)) {
      val head = queue.dequeue()
      val delayedEventTime = head._1

      val now = System.currentTimeMillis()

      val replayTime =
        if (speedupFactor == 0) now
        else toReplayTime(replayStartTime, firstEventTime, delayedEventTime, speedupFactor)

      val waitTime = replayTime - now

      log(s"replay time: $replayTime - delayed event time: $delayedEventTime - wait time: $waitTime - item: ${head._2}")

      if (waitTime > 0) wait(waitTime)

      head._2 match {
        case Left((event, timestamp)) => emitEvent(event, timestamp)
        case Right(watermark) =>
          emitWatermark(watermark)

          // if not cancelled: schedule next watermark if there are events left in the queue or if the queue is empty,
          // but the previous watermark does not cover the maximum event time
          if (!isCancelled() && (queue.nonEmpty || maximumEventTime > watermark.getTimestamp)) {
            scheduleWatermark(delayedEventTime)
          }
      }
    }

    // return delayed timestamp of upcoming event in queue (None if queue is empty or next scheduled item is a watermark)
    if (queue.nonEmpty && queue.head._2.isLeft) Some(queue.head._1) else None
  }

  def updateMaximumEventTime(timestamp: Long): Unit = maximumEventTime = math.max(maximumEventTime, timestamp)

  private def scheduleWatermark(delayedEventTime: Long): Unit = {
    val nextEmitTime = delayedEventTime + watermarkIntervalMillis
    val nextEventTime = nextEmitTime - maximumDelayMillis - 1
    val nextWatermark = new Watermark(nextEventTime)

    queue += ((nextEmitTime, Right(nextWatermark)))
  }

  private def log(msg: => String): Unit = if (LOG.isDebugEnabled()) LOG.debug(msg)
}

object EventScheduler {
  def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long, speedupFactor: Double): Long = {
    require(speedupFactor > 0, s"invalid speedup factor: $speedupFactor")

    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }
}
