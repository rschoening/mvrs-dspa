package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

class AggregateFeatures extends CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent] {
  private val frequencyStateDescriptor = new ValueStateDescriptor("event-frequency", classOf[Int])
  // TODO TTLState

  override def processElement1(value: FeaturizedEvent, ctx: CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent]#Context,
                               out: Collector[FeaturizedEvent]): Unit = {
    val frequencyForPerson = getRuntimeContext.getState(frequencyStateDescriptor).value()

    value.features.append(frequencyForPerson) // in place!

    out.collect(value)
  }

  override def processElement2(value: (Long, Int), ctx: CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent]#Context,
                               out: Collector[FeaturizedEvent]): Unit = {
    getRuntimeContext.getState(frequencyStateDescriptor).update(value._2)
  }
}
