package org.mvrs.dspa.clustering

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.mvrs.dspa.clustering.KMeansClusterFunction._
import org.mvrs.dspa.clustering.KMeansClustering.Point

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KMeansClusterFunction(k: Int) extends ProcessWindowFunction[mutable.ArrayBuffer[Double], (Long, Int, ClusterModel), Int, TimeWindow] {
  require(k > 1, s"invalid k: $k")
  val decay = 0.5

  private val clusterStateDescriptor = new ValueStateDescriptor("cluster-model", classOf[ClusterModel])

  override def process(key: Int,
                       context: Context,
                       elements: Iterable[ArrayBuffer[Double]],
                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    val clusterState = getRuntimeContext.getState(clusterStateDescriptor)

    val points = elements.map(features => Point(features.toVector)).toSeq

    if (points.isEmpty) {
      // don't emit anything, as the clusters are stored in broadcast state downstream, and there is no reliance on
      // watermark updates from the broadcast stream
      return
    }

    // TODO take in control stream for value of K and decay
    // - source: config file read using FileProcessingMode.PROCESS_CONTINUOUSLY -> re-emit on any change

    val previousClusterModel = Option(clusterState.value())

    val newClusterModel = cluster(points, previousClusterModel, decay, k)

    clusterState.update(newClusterModel)

    out.collect((context.window.maxTimestamp(), points.size, newClusterModel))

    // TODO emit information about cluster movement
    //   - as metric?
    //   - on side output stream?
    // - metric seems more appropriate; however the metric will have to be an aggregate (maximum and average distances?)
  }

}

object KMeansClusterFunction {
  /**
    * calculate cluster model based on new points, the previous model and the decay factor
    *
    * @param points        the points to cluster
    * @param previousModel the previous cluster model (optional)
    * @param decay         the decay factor for the previous cluster model
    * @param k             the number of clusters
    * @return the new cluster model
    */
  def cluster(points: Seq[Point], previousModel: Option[ClusterModel], decay: Double, k: Int): ClusterModel = {
    val initialCentroids =
      previousModel
        .map(_.clusters.map(_.centroid))
        .getOrElse(KMeansClustering.createRandomCentroids(points, k))

    val clusters =
      KMeansClustering
        .buildClusters(points, initialCentroids)
        .zipWithIndex
        .map { case ((centroid, clusterPoints), index) => Cluster(index, centroid, clusterPoints.size) }

    previousModel
      .map(_.update(clusters, decay))
      .getOrElse(ClusterModel(clusters.toVector))
  }
}
