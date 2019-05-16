package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.utils.DateTimeUtils

class ProgressMonitorFunction[I]() extends ProcessFunction[I, (I, ProgressInfo)] {
  @transient private var counter: Counter = _
  @transient private var noWatermarkCounter: Counter = _
  @transient private var behindNewestCounter: Counter = _
  @transient private var lateEventCounter: Counter = _
  @transient private var maxTimestamp: Long = _
  @transient private var maximumLateness: Long = _
  @transient private var maximumBehindness: Long = _

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup
    counter = group.counter("elementCounter") // equal to standard counter
    noWatermarkCounter = group.counter("noWatermarkCounter")
    behindNewestCounter = group.counter("behindNewestCounter")
    lateEventCounter = group.counter("lateEventCounter")
  }

  override def processElement(value: I,
                              ctx: ProcessFunction[I, (I, ProgressInfo)]#Context, out: Collector[(I, ProgressInfo)]): Unit = {
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

    // TODO detect / report that the watermark has advanced

    out.collect(
      (
        value,
        ProgressInfo(
          getRuntimeContext.getIndexOfThisSubtask,
          timestamp,
          watermark,
          maxTimestamp,
          counter.getCount,
          lateEventCounter.getCount,
          behindNewestCounter.getCount,
          noWatermarkCounter.getCount,
          maximumLateness,
          maximumBehindness
        )
      )
    )
  }
}

case class ProgressInfo(subtask: Int,
                        timestamp: Long,
                        watermark: Long,
                        maximumTimestamp: Long,
                        totalCountSoFar: Long,
                        lateCountSoFar: Long,
                        behindNewestCountSoFar: Long,
                        noWatermarkCountSoFar: Long,
                        maximumLatenessSoFar: Long,
                        maximumBehindnessSoFar: Long) {
  // NOTE: timestamp == watermark would also be considered late (watermark indicates that no events with t_event <= t_watermark will be observed)
  // however it seems that the Kafka consumer issues the watermarks such that this can occur even on ordered topics
  def isLate: Boolean = timestamp < watermark

  def hasWatermark: Boolean = watermark != Long.MinValue

  def millisBehindWatermark: Long = watermark - timestamp

  def isBehindNewest: Boolean = timestamp < maximumTimestamp

  def millisBehindNewest: Long = maximumTimestamp - timestamp

  override def toString: String = s"subtask: $subtask " +
    s"ts: ${DateTimeUtils.formatTimestamp(timestamp)} " +
    (if (isBehindNewest) s"- behind by: ${DateTimeUtils.formatDuration(millisBehindNewest)} " else " - ") +
    (if (isLate) s"- late by: ${DateTimeUtils.formatDuration(millisBehindWatermark)} " else " - ") +
    (if (hasWatermark) "NO WM " else s"watermark: ${DateTimeUtils.formatTimestamp(watermark)} ") +
    s"| late: $lateCountSoFar " +
    s"- behind: $behindNewestCountSoFar " +
    s"- total: $totalCountSoFar " +
    s"""- no watermark: $noWatermarkCountSoFar """ +
    s"| max. lateness: ${DateTimeUtils.formatDuration(maximumLatenessSoFar)} " +
    s"- max. behindness: ${DateTimeUtils.formatDuration(maximumBehindnessSoFar)}"
}
