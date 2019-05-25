package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.model
import org.mvrs.dspa.model.{ClassifiedEvent, ClusterModel, Point}

/**
  *
  * @param clusterStateDescriptor
  */
class ClassifyEventsFunction(clusterStateDescriptor: MapStateDescriptor[Int, (Long, Int, ClusterModel)])
  extends KeyedBroadcastProcessFunction[Long, FeaturizedEvent, (Long, Int, ClusterModel), ClassifiedEvent] {

  override def processElement(value: FeaturizedEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, FeaturizedEvent, (Long, Int, ClusterModel), ClassifiedEvent]#ReadOnlyContext,
                              out: Collector[ClassifiedEvent]): Unit = {
    val state = ctx.getBroadcastState(clusterStateDescriptor)

    val clusterState = state.get(0)

    // TODO check cause for gaps in classified events - are there really no comments?

    if (clusterState == null) {
      // no clusters yet, cannot classify
    }
    else out.collect(
      model.ClassifiedEvent(
        personId = value.personId,
        eventType = value.eventType,
        eventId = value.eventId,
        cluster = clusterState._3.classify(Point(value.features.toVector)),
        timestamp = value.timestamp
      )
    )
  }

  override def processBroadcastElement(value: (Long, Int, ClusterModel),
                                       ctx: KeyedBroadcastProcessFunction[Long, FeaturizedEvent, (Long, Int, ClusterModel), ClassifiedEvent]#Context,
                                       out: Collector[ClassifiedEvent]): Unit = {
    val state = ctx.getBroadcastState(clusterStateDescriptor)

    state.put(0, value)
  }
}
