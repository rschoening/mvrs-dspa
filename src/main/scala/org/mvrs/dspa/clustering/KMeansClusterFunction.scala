package org.mvrs.dspa.clustering

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.mvrs.dspa.clustering.KMeansClustering.Point

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KMeansClusterFunction(k: Int) extends ProcessWindowFunction[mutable.ArrayBuffer[Double], (Long, Int, ClusterModel), Int, TimeWindow] {
  require(k > 1, s"invalid k: $k")

  private val clusterStateDescriptor = new ValueStateDescriptor("cluster-model", classOf[ClusterModel])

  override def process(key: Int,
                       context: Context,
                       elements: Iterable[ArrayBuffer[Double]],
                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    val clusterState = getRuntimeContext.getState(clusterStateDescriptor)

    val points = elements.map(features => Point(features.toVector)).toSeq

    if (points.isEmpty) {
      // TODO emit previous clusters?
      return
    }

    val previousClusterModel = clusterState.value()

    val clusters = KMeansClustering.buildClusters(points, k)


    // TODO take in control stream for value of K
    // - source: config file read using FileProcessingMode.PROCESS_CONTINUOUSLY -> re-emit on any change

    // TODO get maximum event time of elements seen so far? not meaningful, better to issue the number of points clustered (?)
    val timestamp = context.window.maxTimestamp()

    val outputClusters =
      clusters
        .zipWithIndex
        .map { case ((centroid, ps), index) => Cluster(index, centroid, ps.size) }

    val clusterModel = ClusterModel(outputClusters.toSeq)

    clusterState.update(clusterModel)

    out.collect((timestamp, points.size, clusterModel))

    // TODO store clusters in operator state
    // - use to initialize new clusters?
    // - produce weighted average between last clusters and current clusters (weighted by cluster size and decay factor)

    // TODO emit information about cluster movement
    //   - as metric?
    //   - on side output stream?
    // - metric seems more appropriate; however the metric will have to be an aggregate (maximum and average distances?)
  }
}
