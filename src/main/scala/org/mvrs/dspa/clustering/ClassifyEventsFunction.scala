package org.mvrs.dspa.clustering

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.clustering.KMeansClustering.Point
import org.mvrs.dspa.clustering.UnusualActivityDetectionJob.Cluster

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ClassifyEventsFunction(clusterStateDescriptor: MapStateDescriptor[Int, (Long, Int, Seq[Cluster])])
  extends KeyedBroadcastProcessFunction[Long, (Long, Long, mutable.ArrayBuffer[Double]), (Long, Int, Seq[Cluster]), (Long, Long, Cluster)] {

  override def processElement(value: (Long, Long, ArrayBuffer[Double]),
                              ctx: KeyedBroadcastProcessFunction[Long, (Long, Long, mutable.ArrayBuffer[Double]), (Long, Int, Seq[Cluster]), (Long, Long, Cluster)]#ReadOnlyContext,
                              out: Collector[(Long, Long, Cluster)]): Unit = {
    val state = ctx.getBroadcastState[Int, (Long, Int, Seq[Cluster])](clusterStateDescriptor)

    val clusterState = state.get(0)

    if (clusterState == null) {
      // no clusters yet, cannot classify
    }
    else {
      val currentPoint = Point(value._3.toVector)

      // TODO use KMeansModel abstraction with predict method
      val nearestCluster = clusterState._3 minBy (_.centroid squaredDistanceTo currentPoint)

      out.collect(
        value._1, // person id
        value._2, // comment id
        nearestCluster)
    }
  }

  override def processBroadcastElement(value: (Long, Int, Seq[Cluster]),
                                       ctx: KeyedBroadcastProcessFunction[Long, (Long, Long, ArrayBuffer[Double]), (Long, Int, Seq[Cluster]), (Long, Long, Cluster)]#Context,
                                       out: Collector[(Long, Long, Cluster)]): Unit = {
    val state = ctx.getBroadcastState[Int, (Long, Int, Seq[Cluster])](clusterStateDescriptor)

    state.put(0, value)
  }
}
