package org.mvrs.dspa.clustering

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.mvrs.dspa.clustering.KMeansClustering.Point
import org.mvrs.dspa.clustering.UnusualActivityDetectionJob.Cluster

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KMeansClusterFunction(k: Int) extends ProcessWindowFunction[mutable.ArrayBuffer[Double], (Long, Int, Seq[Cluster]), Int, TimeWindow] {
  require(k > 1, s"invalid k: $k")

  override def process(key: Int,
                       context: Context,
                       elements: Iterable[ArrayBuffer[Double]],
                       out: Collector[(Long, Int, Seq[Cluster])]): Unit = {
    val points = elements.map(v => Point(v.toVector)).toSeq

    val clusters = KMeansClustering.buildClusters(points, k)

    // TODO get maximum event time of elements seen so far? not meaningful, better to issue the number of points clustered (?)
    val timestamp = context.window.maxTimestamp()

    val outputClusters =
      clusters
        .zipWithIndex
        .map { case ((centroid, ps), index) => Cluster(index, centroid, ps.size) }
        .toSeq

    out.collect((timestamp, points.size, outputClusters))

    // TODO store clusters in operator state
    // - use to initialize new clusters?
    // - produce weighted average between last clusters and current clusters (weighted by cluster size and decay factor)

    // TODO emit information about cluster movement
    //   - as metric?
    //   - on side output stream?
    // - metric seems more appropriate; however the metric will have to be an aggregate (maximum and average distances?)
  }
}
