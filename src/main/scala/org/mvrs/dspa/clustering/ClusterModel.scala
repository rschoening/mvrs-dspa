package org.mvrs.dspa.clustering

import org.mvrs.dspa.clustering.KMeansClustering.Point

case class ClusterModel(clusters: Seq[Cluster]) {
  require(clusters.nonEmpty, "empty cluster sequence")

  def classify(point: Point): Cluster = clusters minBy (_.centroid squaredDistanceTo point)
}

case class Cluster(index: Int, centroid: Point, size: Int)
