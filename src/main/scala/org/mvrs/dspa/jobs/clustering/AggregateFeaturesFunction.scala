package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state.{StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
  * Transforms an input stream of featurized events by adding a feature representing the number of posts and comments
  * for the corresponding person in a sliding window. The
  *
  * @param ignoreActivityOlderThan A duration in event time to identify frequency results that are too old to be
  *                                relevant (the current featurized event will in most cases be represented by a
  *                                later frequency result). Note that this does not apply to the individual activity
  *                                events, but the end time of the window within which activity was aggregated.
  * @param stateTtl                The time-to-live for the keyed state representing the latest known event/post
  *                                frequency of the person. This ensures eventual state eviction when a person
  *                                becomes inactive.
  */
class AggregateFeaturesFunction(ignoreActivityOlderThan: Time, stateTtl: Time)
  extends CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent] {

  @transient private lazy val frequencyStateDescriptor: ValueStateDescriptor[(Int, Long)] =
    new ValueStateDescriptor("event-frequency", createTypeInformation[(Int, Long)])

  override def processElement1(value: FeaturizedEvent,
                               ctx: CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent]#Context,
                               out: Collector[FeaturizedEvent]): Unit = {
    val state = getRuntimeContext.getState(frequencyStateDescriptor)
    val stateValue = state.value()

    // get the frequency of past events by the person
    val frequency =
      if (stateValue == null) 0 // no state, frequency = 0
      else stateValue match {
        case (lastKnownFrequency, frequencyTimestamp) =>
          if (frequencyTimestamp < ctx.timestamp() - ignoreActivityOlderThan.toMilliseconds) 0 // too old
          else lastKnownFrequency
      }

    // NOTE: a state value may not be deleted by setting it to null (as this results in NullPointerExceptions during
    //       checkpoints). Apparently one must solely rely on TTL.

    value.features.append(frequency) // in place, features is mutable ArrayBuffer

    out.collect(value)
  }

  override def processElement2(value: (Long, Int),
                               ctx: CoProcessFunction[FeaturizedEvent, (Long, Int), FeaturizedEvent]#Context,
                               out: Collector[FeaturizedEvent]): Unit = {
    getRuntimeContext.getState(frequencyStateDescriptor).update(value._2, ctx.timestamp())
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
