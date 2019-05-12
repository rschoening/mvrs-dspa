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
  require(speedupFactor >= 0, s"invalid speedup factor: $speedupFactor")

  def this(extractEventTime: I => Long, speedupFactor: Double) =
    this(
      extractEventTime,
      speedupFactor,
      waitTime => if (waitTime > 0) Thread.sleep(waitTime)
    )

  /**
    * The start of the replay (lazy, determined on first access)
    */
  @transient private var replayStartTime = 0L

  @transient private var firstEventTime = 0L

  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[SimpleScaledReplayFunction[I]])

  private def log(msg: => String): Unit = if (LOG.isDebugEnabled()) LOG.debug(msg)

  override def map(value: I): I = {
    if (speedupFactor == 0) value
    else delay(value)
  }

  private def delay(value: I): I = {
    require(speedupFactor > 0, "positive speedup factor expected")

    val eventTime = extractEventTime(value)

    val now = System.currentTimeMillis()

    if (replayStartTime == 0L) {
      firstEventTime = eventTime
      replayStartTime = now
    }
    else {
      val replayTime = EventScheduler.toReplayTime(replayStartTime, firstEventTime, eventTime, speedupFactor)

      val waitTime = replayTime - now

      log(s"replay time: $replayTime - event time: $eventTime - wait time: $waitTime - item: $value")

      if (waitTime > 0) wait(waitTime)
    }

    value
  }
}