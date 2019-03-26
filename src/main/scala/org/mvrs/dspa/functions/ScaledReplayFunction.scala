package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class ScaledReplayFunction[I](extractEventTime: I => Long, speedupFactor: Double, randomDelay: Double) extends ProcessFunction[I, I] {
  private lazy val replayStartTime: Long = System.currentTimeMillis
  private val queue = mutable.PriorityQueue.empty[(Long, I)](Ordering.by((_: (Long, I))._1).reverse)
  private var firstEventTime = Long.MinValue

  override def processElement(value: I, ctx: ProcessFunction[I, I]#Context, out: Collector[I]): Unit = {
    val eventTime: Long = extractEventTime(value)

    assert(eventTime != Long.MinValue, s"invalid event time: $eventTime")

    val now = System.currentTimeMillis

    if (firstEventTime == Long.MinValue) {
      firstEventTime = eventTime // first message, don't delay
    }
    else {
      val replayWait = if (speedupFactor == 0) 0 else toReplayTime(replayStartTime, firstEventTime, eventTime) - now

      // TODO add random delay (use priority queue to schedule replay in delayed processing time order)

      // NOTE random delay: assume that the input processing time differences between subsequent events are negligible compared to the simulated output delays
      //      (otherwise timers would be needed, which would require the stream to be keyed)

      if (replayWait > 0) {
        Thread.sleep(replayWait)
      }
    }

    out.collect(value)
  }

  private def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long): Long = {
    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }
}
