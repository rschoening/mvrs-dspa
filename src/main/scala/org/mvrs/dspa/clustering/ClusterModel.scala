package org.mvrs.dspa.clustering

import org.mvrs.dspa.clustering.KMeansClustering.Point

case class ClusterModel(clusters: Vector[Cluster]) {
  require(clusters.nonEmpty, "empty cluster sequence")

  def update(updateClusters: Iterable[Cluster], decay: Double): ClusterModel = {
    val newClusters =
      updateClusters
        .view
        .zipWithIndex
        .map {
          case (cluster, index) =>
            if (index >= clusters.size) cluster // no matching old cluster, use as is
            else update(clusters(index), cluster, decay)
        }
        .toVector

    // NOTE cluster weights can get very small if no points are assigned to a cluster - split largest cluster in this case

    ClusterModel(newClusters)
  }

  def classify(point: Point): Cluster = clusters minBy (_.centroid squaredDistanceTo point)

  private def update(oldCluster: Cluster, updateCluster: Cluster, decay: Double): Cluster = {
    val newWeight = oldCluster.weight * decay + updateCluster.weight

    Cluster(
      oldCluster.index,
      Point(
        oldCluster.centroid.features
          .zip(updateCluster.centroid.features) // same dimensions assumed; otherwise output reduced to minimum dim.
          .map {
          case (oldValue, newValue) =>
            (newValue * updateCluster.weight + oldValue * oldCluster.weight * decay) / newWeight
        }
      ),
      newWeight)
  }
}

case class Cluster(index: Int, centroid: Point, weight: Double)
