package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.{Counter, Meter}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.utils.DateTimeUtils

class ProgressMonitorFunction[I]() extends ProcessFunction[I, (I, ProgressInfo)] {
  @transient private var counter: Counter = _
  @transient private var noWatermarkCounter: Counter = _
  @transient private var behindNewestCounter: Counter = _
  @transient private var lateEventCounter: Counter = _
  @transient private var watermarkAdvancedCounter: Counter = _
  @transient private var watermarkAdvancedPerSecond: Meter = _

  @transient private var maxTimestamp: Long = _
  @transient private var maximumLateness: Long = _
  @transient private var maximumBehindness: Long = _
  @transient private var previousWatermark: Long = _

  @transient private var startTimeNanos: Long = _
  @transient private var previousWatermarkEmitTimeNanos: Long = _
  @transient private var previousEventEmitTimeNanos: Long = _

  @transient private var sumOfProcTimeBetweenEventsMillis: Double = 0.0
  @transient private var sumOfProcTimeBetweenWatermarksMillis: Double = 0.0

  @transient lazy val nanosPerMilli: Double = 1000.0 * 1000.0

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    counter = group.counter("elementCounter") // equal to standard counter
    noWatermarkCounter = group.counter("noWatermarkCounter")
    behindNewestCounter = group.counter("behindNewestCounter")
    lateEventCounter = group.counter("lateEventCounter")
    watermarkAdvancedCounter = group.counter("watermarkAdvancedCounter")
    watermarkAdvancedPerSecond = group.meter("watermarkAdvancedPerSecond",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    previousWatermarkEmitTimeNanos = Long.MinValue
    previousEventEmitTimeNanos = Long.MinValue
    previousWatermark = Long.MinValue
    startTimeNanos = System.nanoTime()
  }

  override def processElement(value: I,
                              ctx: ProcessFunction[I, (I, ProgressInfo)]#Context,
                              out: Collector[(I, ProgressInfo)]): Unit = {
    val timestamp = ctx.timestamp()
    val watermark = ctx.timerService().currentWatermark()

    val isLate = timestamp < watermark // actually, <=
    counter.inc()

    if (timestamp < maxTimestamp) behindNewestCounter.inc()
    if (isLate) lateEventCounter.inc()
    if (watermark == Long.MinValue) noWatermarkCounter.inc()

    maximumBehindness = math.max(maximumBehindness, maxTimestamp - timestamp)
    if (isLate) maximumLateness = math.max(maximumLateness, watermark - timestamp)
    maxTimestamp = math.max(timestamp, maxTimestamp)

    val nowNanos = System.nanoTime()

    if (previousEventEmitTimeNanos != Long.MinValue) {
      val nanosSincePreviousEvent = nowNanos - previousEventEmitTimeNanos
      sumOfProcTimeBetweenEventsMillis = sumOfProcTimeBetweenEventsMillis + nanosSincePreviousEvent / nanosPerMilli
    }
    previousEventEmitTimeNanos = nowNanos

    val watermarkAdvanced = watermark > previousWatermark
    if (watermarkAdvanced) {
      if (previousWatermark != Long.MinValue) {
        val nanosSincePreviousWatermark = nowNanos - previousWatermarkEmitTimeNanos
        sumOfProcTimeBetweenWatermarksMillis = sumOfProcTimeBetweenWatermarksMillis + nanosSincePreviousWatermark / nanosPerMilli
      }

      previousWatermark = watermark
      previousWatermarkEmitTimeNanos = nowNanos
      watermarkAdvancedPerSecond.markEvent()
      watermarkAdvancedCounter.inc()
    }

    val watermarkCount = watermarkAdvancedCounter.getCount
    val eventCount = counter.getCount

    val avgWatermarksPerSecond =
      if (sumOfProcTimeBetweenWatermarksMillis == 0)
        Double.NaN
      else
        watermarkCount.toDouble / (sumOfProcTimeBetweenWatermarksMillis / 1000)

    val avgEventsPerSecond =
      if (sumOfProcTimeBetweenEventsMillis == 0)
        Double.NaN
      else
        eventCount.toDouble / (sumOfProcTimeBetweenEventsMillis / 1000)

    out.collect(
      (
        value,
        ProgressInfo(
          getRuntimeContext.getIndexOfThisSubtask,
          timestamp,
          watermark,
          watermarkAdvanced,
          maxTimestamp,
          eventCount,
          lateEventCounter.getCount,
          behindNewestCounter.getCount,
          noWatermarkCounter.getCount,
          watermarkCount,
          maximumLateness,
          maximumBehindness,
          nowNanos - startTimeNanos,
          avgWatermarksPerSecond,
          avgEventsPerSecond
        )
      )
    )
  }
}

case class ProgressInfo(subtask: Int,
                        timestamp: Long,
                        watermark: Long,
                        watermarkAdvanced: Boolean,
                        maximumTimestamp: Long,
                        totalCountSoFar: Long,
                        lateCountSoFar: Long,
                        behindNewestCountSoFar: Long,
                        noWatermarkCountSoFar: Long,
                        watermarkAdvancedCount: Long,
                        maximumLatenessSoFar: Long,
                        maximumBehindnessSoFar: Long,
                        nanosSinceStart: Long,
                        avgWatermarksPerSecond: Double,
                        avgEventsPerSecond: Double) {
  // NOTE: timestamp == watermark would also be considered late (watermark indicates that no events with t_event <= t_watermark will be observed)
  // however it seems that the Kafka consumer issues the watermarks such that this can occur even on ordered topics
  def isLate: Boolean = timestamp < watermark

  def hasWatermark: Boolean = watermark != Long.MinValue

  def millisBehindWatermark: Long = watermark - timestamp

  def isBehindNewest: Boolean = timestamp < maximumTimestamp

  def millisBehindNewest: Long = maximumTimestamp - timestamp

  override def toString: String = s"[$subtask] " +
    s"ts: ${DateTimeUtils.formatTimestamp(timestamp, shortFormat = true)} " +
    (if (isBehindNewest)
      s"| behind by: ${DateTimeUtils.formatDuration(millisBehindNewest, shortFormat = true)} "
    else
      "| latest").padTo(24,' ') +
    (if (isLate)
      s"| late by: ${DateTimeUtils.formatDuration(millisBehindWatermark, shortFormat = true)} "
    else
      "| on time").padTo(22, ' ') +
    (if (!hasWatermark)
      "| NO watermark "
    else
      s"| wm: ${DateTimeUtils.formatTimestamp(watermark, shortFormat = true)} ") +
    (if (watermarkAdvanced) "+ " else "= ") +
    s"| late: $lateCountSoFar ".padTo(13, ' ') +
    s"| behind: $behindNewestCountSoFar ".padTo(15, ' ') +
    s"| events: $totalCountSoFar ".padTo(16, ' ') +
    s"| no wm: $noWatermarkCountSoFar " +
    s"| wm+: $watermarkAdvancedCount ".padTo(11, ' ') +
    s"| max. late: ${if (maximumLatenessSoFar == 0) '-' else DateTimeUtils.formatDuration(maximumLatenessSoFar, shortFormat = true)} " +
    s"| max. behind: ${if (maximumBehindnessSoFar == 0) '-' else DateTimeUtils.formatDuration(maximumBehindnessSoFar, shortFormat = true)} " +
    s"| time since start: ${DateTimeUtils.formatDuration(nanosSinceStart / 1000 / 1000, shortFormat = true)} " +
    s"| wm+/sec: ${math.round(avgWatermarksPerSecond * 10) / 10.0} ".padTo(17, ' ') +
    s"| events/sec: ${math.round(avgEventsPerSecond * 10) / 10.0} "
}
