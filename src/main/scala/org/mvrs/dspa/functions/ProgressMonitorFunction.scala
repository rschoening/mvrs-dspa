package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Counter, Meter}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}

class ProgressMonitorFunction[I]() extends ProcessFunction[I, (I, ProgressInfo)] {
  // metrics
  @transient private var elementCounter: Counter = _
  @transient private var noWatermarkCounter: Counter = _
  @transient private var behindNewestCounter: Counter = _
  @transient private var lateElementsCounter: Counter = _
  @transient private var watermarkAdvancedCounter: Counter = _
  @transient private var watermarkAdvancedPerSecond: Meter = _
  @transient private var watermarkIncrementHistogram: DropwizardHistogramWrapper = _
  @transient private var latenessHistogram: DropwizardHistogramWrapper = _
  @transient private var behindNewestHistogram: DropwizardHistogramWrapper = _

  // State (not checkpointed, as this function is currently debug-only and does not need to be fault-tolerant).
  // Maximum values represent values since last restore. Checkpointing can be easily added if ever needed.
  @transient private var maximumTimestamp: Long = _
  @transient private var maximumLateness: Long = _
  @transient private var maximumBehindNewest: Long = _
  @transient private var maximumWatermarkIncrement: Long = _
  @transient private var previousWatermark: Long = _

  @transient private var startTimeNanos: Long = _
  @transient private var previousWatermarkEmitTimeNanos: Long = _
  @transient private var previousEventEmitTimeNanos: Long = _

  @transient private var sumOfProcTimeBetweenEventsMillis: Double = 0.0
  @transient private var sumOfProcTimeBetweenWatermarksMillis: Double = 0.0

  @transient private lazy val nanosPerMilli: Double = 1000.0 * 1000.0 // constant to convert to milliseconds

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    elementCounter = group.counter("elementCounter") // equal to standard counter
    noWatermarkCounter = group.counter("noWatermarkCounter")
    behindNewestCounter = group.counter("behindNewestCounter")
    lateElementsCounter = group.counter("lateEventCounter")
    watermarkAdvancedCounter = group.counter("watermarkAdvancedCounter")

    watermarkAdvancedPerSecond = group.meter("watermarkAdvancedPerSecond",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    watermarkIncrementHistogram = FlinkUtils.histogramMetric("watermarkIncrementHistogram", group)
    latenessHistogram = FlinkUtils.histogramMetric("latenessHistogram", group)
    behindNewestHistogram = FlinkUtils.histogramMetric("behindNewestHistogram", group)

    previousWatermarkEmitTimeNanos = Long.MinValue
    previousEventEmitTimeNanos = Long.MinValue
    previousWatermark = Long.MinValue
    startTimeNanos = System.nanoTime()
  }

  override def processElement(value: I,
                              ctx: ProcessFunction[I, (I, ProgressInfo)]#Context,
                              out: Collector[(I, ProgressInfo)]): Unit = {
    val elementTimestamp = ctx.timestamp()
    val watermark = ctx.timerService().currentWatermark()

    val isLate = elementTimestamp < watermark // actually, <=
    val isBehindNewest = elementTimestamp < maximumTimestamp
    val watermarkAdvanced = watermark > previousWatermark
    val hasPreviousWatermark = previousWatermark != Long.MinValue
    val watermarkIncrement = if (watermarkAdvanced && hasPreviousWatermark) watermark - previousWatermark else 0L
    val lateness = if (watermark == Long.MinValue) 0 else math.max(watermark - elementTimestamp, 0)
    val behindNewest = maximumTimestamp - elementTimestamp

    maximumBehindNewest = math.max(maximumBehindNewest, behindNewest)
    maximumLateness = math.max(maximumLateness, lateness)
    maximumTimestamp = math.max(elementTimestamp, maximumTimestamp)
    maximumWatermarkIncrement = math.max(watermarkIncrement, maximumWatermarkIncrement)
    // metrics
    elementCounter.inc()
    if (isBehindNewest) behindNewestCounter.inc()
    if (isLate) lateElementsCounter.inc()
    if (watermark == Long.MinValue) noWatermarkCounter.inc()
    if (watermarkAdvanced) watermarkAdvancedPerSecond.markEvent()
    if (watermarkAdvanced) watermarkAdvancedCounter.inc()
    if (watermarkAdvanced && hasPreviousWatermark) watermarkIncrementHistogram.update(watermarkIncrement)

    latenessHistogram.update(lateness)
    behindNewestHistogram.update(behindNewest)

    // emit time differences
    val nowNanos = System.nanoTime()

    if (previousEventEmitTimeNanos != Long.MinValue) {
      val nanosSincePreviousEvent = nowNanos - previousEventEmitTimeNanos
      sumOfProcTimeBetweenEventsMillis = sumOfProcTimeBetweenEventsMillis + nanosSincePreviousEvent / nanosPerMilli
    }
    previousEventEmitTimeNanos = nowNanos

    if (watermarkAdvanced) {
      if (hasPreviousWatermark) {
        val nanosSincePreviousWatermark = nowNanos - previousWatermarkEmitTimeNanos
        sumOfProcTimeBetweenWatermarksMillis = sumOfProcTimeBetweenWatermarksMillis + nanosSincePreviousWatermark / nanosPerMilli
      }

      previousWatermark = watermark
      previousWatermarkEmitTimeNanos = nowNanos
    }

    val watermarkCount = watermarkAdvancedCounter.getCount
    val elementCount = elementCounter.getCount

    val avgWatermarksPerSecond =
      if (sumOfProcTimeBetweenWatermarksMillis == 0)
        Double.NaN
      else
        watermarkCount.toDouble / (sumOfProcTimeBetweenWatermarksMillis / 1000) // incorrect after restore from checkpoint (metric is checkpointed, sum is not)

    val avgEventsPerSecond =
      if (sumOfProcTimeBetweenEventsMillis == 0)
        Double.NaN
      else
        elementCount.toDouble / (sumOfProcTimeBetweenEventsMillis / 1000) // incorrect after restore from checkpoint (metric is checkpointed, sum is not)

    out.collect(
      (
        value,
        ProgressInfo(
          getRuntimeContext.getIndexOfThisSubtask,
          elementTimestamp,
          watermark,
          watermarkAdvanced,
          watermarkIncrement,
          maximumTimestamp,
          elementCount,
          lateElementsCounter.getCount,
          behindNewestCounter.getCount,
          noWatermarkCounter.getCount,
          watermarkCount,
          maximumLateness,
          maximumBehindNewest,
          maximumWatermarkIncrement,
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
                        watermarkIncrement: Long,
                        maximumTimestamp: Long,
                        elementCount: Long,
                        lateElementsCount: Long,
                        elementsBehindNewestCount: Long,
                        noWatermarkCount: Long,
                        watermarkAdvancedCount: Long,
                        maximumLateness: Long,
                        maximumBehindNewest: Long,
                        maximumWatermarkIncrement: Long,
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
      s"| bn: ${DateTimeUtils.formatDuration(millisBehindNewest, shortFormat = true)} "
    else
      "|  * latest *").padTo(16, ' ') +
    (if (isLate)
      s"| lt: ${DateTimeUtils.formatDuration(millisBehindWatermark, shortFormat = true)} "
    else
      "|  * on time *").padTo(16, ' ') +
    (if (!hasWatermark)
      "| * NO watermark *"
    else
      s"| wm: ${DateTimeUtils.formatTimestamp(watermark, shortFormat = true)} ").padTo(26, ' ') +
    (if (watermarkAdvanced) "+ " else "= ") +
    s"| wm+: ${if (watermarkIncrement == 0) '-' else DateTimeUtils.formatDuration(watermarkIncrement, shortFormat = true)}".padTo(18, ' ') +
    s"| elc: $elementCount ".padTo(14, ' ') +
    s"| ltc: $lateElementsCount ".padTo(13, ' ') +
    s"| bnc: $elementsBehindNewestCount ".padTo(14, ' ') +
    s"| nwm: $noWatermarkCount " +
    s"| wm+: $watermarkAdvancedCount ".padTo(12, ' ') +
    s"| mlt: ${if (maximumLateness == 0) '-' else DateTimeUtils.formatDuration(maximumLateness, shortFormat = true)}".padTo(18, ' ') +
    s"| mbn: ${if (maximumBehindNewest == 0) '-' else DateTimeUtils.formatDuration(maximumBehindNewest, shortFormat = true)}".padTo(18, ' ') +
    s"| mwi: ${if (maximumWatermarkIncrement == 0) '-' else DateTimeUtils.formatDuration(maximumWatermarkIncrement, shortFormat = true)}".padTo(18, ' ') +
    s"| wm+/s: ${math.round(avgWatermarksPerSecond)} ".padTo(13, ' ') +
    s"| elm/s: ${math.round(avgEventsPerSecond)} ".padTo(14, ' ') +
    s"| rt: ${DateTimeUtils.formatDuration(nanosSinceStart / 1000 / 1000, shortFormat = true)} "
}
