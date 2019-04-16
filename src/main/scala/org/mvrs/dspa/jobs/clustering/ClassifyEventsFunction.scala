package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.clustering.KMeansClustering.Point

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ClassifyEventsFunction(clusterStateDescriptor: MapStateDescriptor[Int, (Long, Int, ClusterModel)])
  extends KeyedBroadcastProcessFunction[Long, (Long, Long, mutable.ArrayBuffer[Double]), (Long, Int, ClusterModel), ClassifiedEvent] {

  override def processElement(value: (Long, Long, ArrayBuffer[Double]),
                              ctx: KeyedBroadcastProcessFunction[Long, (Long, Long, mutable.ArrayBuffer[Double]), (Long, Int, ClusterModel), ClassifiedEvent]#ReadOnlyContext,
                              out: Collector[ClassifiedEvent]): Unit = {
    val state = ctx.getBroadcastState(clusterStateDescriptor)

    val clusterState = state.get(0)

    if (clusterState == null) {
      // no clusters yet, cannot classify
    }
    else out.collect(
      ClassifiedComment(
        personId = value._1,
        eventId = value._2,
        cluster = clusterState._3.classify(Point(value._3.toVector)),
        timestamp = ctx.timestamp()
      )
    )
  }

  override def processBroadcastElement(value: (Long, Int, ClusterModel),
                                       ctx: KeyedBroadcastProcessFunction[Long, (Long, Long, ArrayBuffer[Double]), (Long, Int, ClusterModel), ClassifiedEvent]#Context,
                                       out: Collector[ClassifiedEvent]): Unit = {
    val state = ctx.getBroadcastState(clusterStateDescriptor)

    state.put(0, value)
  }
}
