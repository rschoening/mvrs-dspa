package org.mvrs.dspa.model

final case class ClusterModel(clusters: Vector[Cluster]) {
  private val clustersByIndex = clusters.map(c => (c.index, c)).toMap

  require(clusters.nonEmpty, "empty cluster sequence")

  def update(updateClusters: Iterable[Cluster], decay: Double): ClusterModel = {
    val newClusters =
      updateClusters
        .map(cluster =>
          if (!clustersByIndex.contains(cluster.index)) cluster // no matching old cluster, use as is
          else update(clustersByIndex(cluster.index), cluster, decay)
        )
        .toVector

    // NOTE cluster weights can get very small if no points are assigned to a cluster - split largest cluster in this case

    ClusterModel(newClusters)
  }

  def classify(point: Point): Cluster = clusters minBy (_.centroid squaredDistanceTo point)

  private def update(oldCluster: Cluster, updateCluster: Cluster, decay: Double): Cluster = {
    val newWeight = oldCluster.weight * decay + updateCluster.weight

    Cluster(
      updateCluster.index,
      updateCentroid(oldCluster, updateCluster, decay, newWeight),
      newWeight,
      updateCluster.label
    )
  }

  private def updateCentroid(oldCluster: Cluster, updateCluster: Cluster, decay: Double, newWeight: Double): Point =
    Point(
      oldCluster.centroid.features
        .zip(updateCluster.centroid.features) // same dimensions assumed; otherwise output reduced to minimum dim.
        .map { case (oldValue, newValue) => (newValue * updateCluster.weight + oldValue * oldCluster.weight * decay) / newWeight }
    )
}

final case class Cluster(index: Int, centroid: Point, weight: Double, label: Option[String] = None) {
  def labelText: String = label.map(s => s"$index: $s").getOrElse(s"$index")
}

final case class ClusterMetadata(timestamp: Long,
                                 clusters: Vector[(Cluster, Vector[Double], Double, Double)],
                                 averageVectorDistance: Double,
                                 averageWeightDifference: Double,
                                 kDifference: Int) {
  require(! averageVectorDistance.isNaN, "average vector distance is NaN")
  require(! averageWeightDifference.isNaN, "average weight difference is NaN")
}
