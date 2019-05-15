package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class ProgressMonitorFunction[I]() extends ProcessFunction[I, ProgressInfo[I]] {
  @transient private var counter: Counter = _
  @transient private var behindNewestCounter: Counter = _
  @transient private var lateEventCounter: Counter = _
  @transient private var maxTimestamp: Long = _
  @transient private var maximumLateness: Long = _
  @transient private var maximumBehindness: Long = _

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup
    counter = group.counter("elementCounter") // equal to standard counter
    behindNewestCounter = group.counter("behindNewestCounter")
    lateEventCounter = group.counter("lateEventCounter")
  }

  override def processElement(value: I, ctx: ProcessFunction[I, ProgressInfo[I]]#Context, out: Collector[ProgressInfo[I]]): Unit = {
    val timestamp = ctx.timestamp()
    val watermark = ctx.timerService().currentWatermark()

    val isLate = timestamp < watermark // actually, <=

    counter.inc()

    if (timestamp < maxTimestamp) behindNewestCounter.inc()
    if (isLate) lateEventCounter.inc()

    maximumBehindness = math.max(maximumBehindness, maxTimestamp - timestamp)
    if (isLate) maximumLateness = math.max(maximumLateness, watermark - timestamp)
    maxTimestamp = math.max(timestamp, maxTimestamp)

    out.collect(
      ProgressInfo(
        value,
        getRuntimeContext.getIndexOfThisSubtask,
        timestamp,
        watermark,
        maxTimestamp,
        counter.getCount,
        lateEventCounter.getCount,
        behindNewestCounter.getCount,
        maximumLateness,
        maximumBehindness
      )
    )
  }
}

case class ProgressInfo[T](element: T,
                           subtask: Int,
                           timestamp: Long,
                           watermark: Long,
                           maximumTimestamp: Long,
                           totalCountSoFar: Long,
                           lateCountSoFar: Long,
                           behindNewestCountSoFar: Long,
                           maximumLatenessSoFar: Long,
                           maximumBehindnessSoFar: Long) {
  // NOTE: timestamp == watermark would also be considered late (watermark indicates that no events with t_event <= t_watermark will be observed)
  // however it seems that the Kafka consumer issues the watermarks such that this can occur even on ordered topics
  def isLate: Boolean = timestamp < watermark

  def millisBehindWatermark: Long = watermark - timestamp

  def isBehindNewest: Boolean = timestamp < maximumTimestamp

  def millisBehindNewest: Long = maximumTimestamp - timestamp
}
