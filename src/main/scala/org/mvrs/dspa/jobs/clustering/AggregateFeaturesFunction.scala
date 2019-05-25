package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state.{StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
  *
  * @param stateTtl
  */
class AggregateFeaturesFunction(stateTtl: Time) extends CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent] {

  private lazy val frequencyStateDescriptor: ValueStateDescriptor[Int] =
    new ValueStateDescriptor("event-frequency", classOf[Int])

  override def processElement1(value: FeaturizedEvent,
                               ctx: CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent]#Context,
                               out: Collector[FeaturizedEvent]): Unit = {
    val frequencyForPerson = getRuntimeContext.getState(frequencyStateDescriptor).value()

    value.features.append(frequencyForPerson) // in place, features is mutable ArrayBuffer

    out.collect(value)
  }

  override def processElement2(value: (Long, Int),
                               ctx: CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent]#Context,
                               out: Collector[FeaturizedEvent]): Unit = {
    getRuntimeContext.getState(frequencyStateDescriptor).update(value._2)
  }

  override def open(parameters: Configuration): Unit = {
    val ttlConfig = StateTtlConfig
      .newBuilder(stateTtl)
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
      .build

    frequencyStateDescriptor.enableTimeToLive(ttlConfig)
  }
}
