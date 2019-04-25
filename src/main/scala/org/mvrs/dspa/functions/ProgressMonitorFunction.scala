package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.utils.DateTimeUtils

class ProgressMonitorFunction[I](name: String, interval: Int = 1) extends ProcessFunction[I, I] {
  @transient private var counter: Counter = _

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext
      .getMetricGroup
      .counter("element-counter")
  }

  override def processElement(value: I, ctx: ProcessFunction[I, I]#Context, out: Collector[I]): Unit = {
    counter.inc()
    val count = counter.getCount

    if (interval > 0 && count % interval == 0) {
      val watermark = ctx.timerService().currentWatermark()
      val timestamp = ctx.timestamp()
      val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
      val subtaskCount = getRuntimeContext.getNumberOfParallelSubtasks

      println(s"[$name $subtaskIndex/$subtaskCount] wm: ${DateTimeUtils.formatTimestamp(watermark)} " +
        s"- ts: ${DateTimeUtils.formatTimestamp(timestamp)} " +
        s"- wm delay: ${DateTimeUtils.formatDuration(timestamp - watermark)} - per-worker count: $count")
    }

    out.collect(value)
  }
}
