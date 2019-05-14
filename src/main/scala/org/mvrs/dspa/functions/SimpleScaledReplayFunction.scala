package org.mvrs.dspa.functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.{Counter, Gauge, Meter}
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
                                    wait: Long => Unit) extends RichMapFunction[I, I] {
  require(speedupFactor >= 0, s"invalid speedup factor: $speedupFactor")

  def this(extractEventTime: I => Long, speedupFactor: Double) =
    this(
      extractEventTime,
      speedupFactor,
      waitTime => if (waitTime > 0) Thread.sleep(waitTime)
    )

  // metrics
  @transient private var totalWaitTime: Counter = _
  @transient private var waitTimePerSecond: Meter = _
  @transient private var lastTimeDifferenceGauge: Gauge[Long] = _

  /**
    * The start of the replay
    *
    * @note not checkpointed; re-initialized when restarting from checkpoint
    */
  @transient private var replayStartTime = 0L

  /**
    * The event time of the first event in the stream
    *
    * @note not checkpointed; re-initialized when restarting from checkpoint
    */
  @transient private var firstEventTime = 0L

  /**
    * The time difference of the last processed element (relative to scaled replay time). If the time difference is
    * positive, the operator waited for that time to emit the element. If the difference is negative, then the source
    * is slower than the scaled replay time and elements are emitted immediately.
    */
  @transient private var lastTimeDifference = 0L

  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[SimpleScaledReplayFunction[I]])

  private def log(msg: => String): Unit = if (LOG.isDebugEnabled()) LOG.debug(msg)

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    totalWaitTime = group.counter("totalWaitTime")
    waitTimePerSecond = group.meter("waitTimePerSecond",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))
    lastTimeDifferenceGauge = group.gauge[Long, ScalaGauge[Long]]("lastTimeDifference",
      ScalaGauge[Long](() => lastTimeDifference))
  }

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

      val timeDifference = replayTime - now

      log(s"replay time: $replayTime - event time: $eventTime - time difference: $timeDifference - item: $value")

      lastTimeDifference = timeDifference

      if (timeDifference > 0) {
        wait(timeDifference)

        waitTimePerSecond.markEvent(timeDifference)
        totalWaitTime.inc(timeDifference)
      }
    }

    value
  }
}