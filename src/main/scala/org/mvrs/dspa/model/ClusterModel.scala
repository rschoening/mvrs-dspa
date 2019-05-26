package org.mvrs.dspa.model

/**
  * A collection of clusters
  *
  */
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
        .zip(updateCluster.centroid.features) // same dimension count assumed; otherwise output reduced to minimum dim.
        .map {
        case (oldValue, newValue) =>
          (newValue * updateCluster.weight + oldValue * oldCluster.weight * decay) / newWeight
      }
    )
}

/**
  * An individual cluster in the model
  *
  * @param index    The index of the cluster
  * @param centroid The cluster centroid
  * @param weight   The cluster weight, derived from the number of assigned points
  * @param label    The optional cluster label
  */
final case class Cluster(index: Int, centroid: Point, weight: Double, label: Option[String] = None) {
  def labelText: String = label.map(s => s"$index: $s").getOrElse(s"$index")
}

/**
  * Metadata about a cluster model and its differences to the preceding model
  *
  * @param timestamp               Creation time of this version of the model
  * @param clusters                The collection of clusters, with metadata for each cluster (difference vector,
  *                                length of difference vector, weight difference)
  * @param averageVectorDistance   The average length of the difference vectors per cluster
  * @param averageWeightDifference The average weight difference per cluster
  * @param kDifference             The difference of the number of clusters compared to the preceding model
  */
final case class ClusterMetadata(timestamp: Long,
                                 clusters: Vector[(Cluster, Vector[Double], Double, Double)],
                                 averageVectorDistance: Double,
                                 averageWeightDifference: Double,
                                 kDifference: Int) {
  require(!averageVectorDistance.isNaN, "average vector distance is NaN")
  require(!averageWeightDifference.isNaN, "average weight difference is NaN")
}
