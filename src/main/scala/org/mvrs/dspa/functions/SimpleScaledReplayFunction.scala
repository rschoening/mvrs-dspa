package org.mvrs.dspa.functions

import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.LoggerFactory

/**
  * Replay function that only applies replay time scaling, but no reordering
  *
  * @param extractEventTime function to extract the event time for an element
  * @param speedupFactor    the speedup factor to apply relative to the event time
  * @param wait             function to wait for a given duration (in milliseconds). Useful for unit testing
  * @tparam I the type of elements
  * @note when the input is unordered regarding event times, then events with earlier event time than their predecessor
  *       are emitted immediately
  */
class SimpleScaledReplayFunction[I](extractEventTime: I => Long,
                                    speedupFactor: Double,
                                    wait: Long => Unit) extends MapFunction[I, I] {

  def this(extractEventTime: I => Long, speedupFactor: Double) =
    this(
      extractEventTime,
      speedupFactor,
      waitTime => if (waitTime > 0) Thread.sleep(waitTime)
    )

  @transient private var previousEventTime = 0L
  @transient private var previousEmitTime = 0L
  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[SimpleScaledReplayFunction[I]])

  private def log(msg: => String): Unit = if (LOG.isDebugEnabled()) LOG.debug(msg)

  override def map(value: I): I = {

    val eventTime = extractEventTime(value)

    val now = System.currentTimeMillis()

    if (previousEventTime > 0) {
      val eventTimeDiff = eventTime - previousEventTime // may be negative
      val scaledEventTimeDiff = eventTimeDiff / speedupFactor
      val replayTime = previousEmitTime + scaledEventTimeDiff

      val waitTime = replayTime - now

      log(s"replay time: $replayTime - scaled event time difference: $scaledEventTimeDiff - wait time: $waitTime - item: $value")

      if (waitTime > 0) wait(waitTime.ceil.toLong) // round up
    }

    previousEventTime = eventTime
    previousEmitTime = System.currentTimeMillis()

    value
  }
}


