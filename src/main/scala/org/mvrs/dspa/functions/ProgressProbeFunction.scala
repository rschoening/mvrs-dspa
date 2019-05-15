package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.utils.DateTimeUtils

class ProgressProbeFunction[I](name: String, interval: Int = 1) extends ProcessFunction[I, I] {
  @transient private var counter: Counter = _
  @transient private var outOfOrderCounter: Counter = _
  @transient private var lateEventCounter: Counter = _

  @transient private var maxTimestamp: Long = _

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup
    counter = group.counter("element-counter")
    outOfOrderCounter = group.counter("out-of-order-counter")
    lateEventCounter = group.counter("late-event-counter")
  }

  override def processElement(value: I, ctx: ProcessFunction[I, I]#Context, out: Collector[I]): Unit = {
    counter.inc()
    val count = counter.getCount

    val timestamp = ctx.timestamp()
    val watermark = ctx.timerService().currentWatermark()

    if (timestamp < maxTimestamp) outOfOrderCounter.inc()
    if (timestamp < watermark) lateEventCounter.inc()

    if (interval > 0 && count % interval == 0) {

      val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
      val subtaskCount = getRuntimeContext.getNumberOfParallelSubtasks

      val msg = s"[$name $subtaskIndex/$subtaskCount] " +
        s"- wm: ${DateTimeUtils.formatTimestamp(watermark)} " +
        s"- ts: ${DateTimeUtils.formatTimestamp(timestamp)} " +
        s"- wm delay: ${DateTimeUtils.formatDuration(timestamp - watermark)} " +
        (if (timestamp < maxTimestamp) s" - behind max by ${DateTimeUtils.formatTimestamp(maxTimestamp - timestamp)} " else "- max ts ") +
        s"- records: $count " +
        s"- behind max: ${outOfOrderCounter.getCount} " +
        s"- late: ${lateEventCounter.getCount}"

      println(msg)
    }

    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }

    out.collect(value)
  }
}
