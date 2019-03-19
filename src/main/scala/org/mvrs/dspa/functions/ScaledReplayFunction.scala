package org.mvrs.dspa.functions

import java.util.Calendar

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class ScaledReplayFunction[I](extractEventTime: I => Long, speedupFactor: Double, randomDelay: Double) extends ProcessFunction[I, I] {
  private lazy val servingStartTime: Long = Calendar.getInstance.getTimeInMillis
  private var firstEventTime = -1L

  override def processElement(value: I, ctx: ProcessFunction[I, I]#Context, out: Collector[I]): Unit = {
    val eventTime: Long = extractEventTime(value)

    assert(eventTime >= 0, "event time is expected to be >= 0")

    val now = Calendar.getInstance.getTimeInMillis

    if (firstEventTime == -1) {
      firstEventTime = eventTime // first message, don't delay
    }
    else {
      val eventServingTime = toServingTime(servingStartTime, firstEventTime, eventTime)

      val eventWait = eventServingTime - now

      if (eventWait > 0) {
        Thread.sleep(eventWait)
      }
    }

    // TODO add random delay (use priority queue to schedule replay in delayed processing time order)

    // NOTE random delay: assume that the input processing time differences between subsequent events are negligible compared to the simulated output delays
    //      (otherwise timers would be needed, which would require the stream to be keyed)

    out.collect(value)
  }

  private def toServingTime(servingStartTime: Long, dataStartTime: Long, eventTime: Long): Long = {
    val dataDiff = eventTime - dataStartTime
    servingStartTime + (dataDiff / speedupFactor).toLong
  }
}
