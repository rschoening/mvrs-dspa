package org.mvrs.dspa.jobs.clustering

import org.mvrs.dspa.model.Point

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

/**
  * simple K-means implementation
  *
  * Adapted from [[https://gist.github.com/metanet/a385d42fd2cab9f3d20e]]
  *
  */
object KMeansClustering {

  /**
    * create random centroids
    *
    * @param points the points for which to generate random centroids
    * @param k      number of clusters
    * @param random random number generator
    * @return k random centroids
    */
  def createRandomCentroids(points: Iterable[Point], k: Int, random: Random = new Random()): Seq[Point] = {

    val uniquePoints = points.toSet
    val resultSet = mutable.Set[Point]()

    if (uniquePoints.size < k) {
      val dim = getDimensions(points)
      resultSet ++= uniquePoints

      while (resultSet.size < k) {
        resultSet += randomPoint(dim, random)
      }
    }
    else {
      // there are enough unique points, select them by random index
      val indexedPoints = uniquePoints.toIndexedSeq

      while (resultSet.size < k) {
        resultSet += indexedPoints(random.nextInt(indexedPoints.size))
      }
    }

    resultSet.toSeq // to immutable
  }

  private def getDimensions(points: Iterable[Point]) = {
    require(points.nonEmpty, "empty input")
    points.head.features.size
  }

  /**
    * builds the clusters based on randomly generated centroids
    *
    * @param points the points to cluster
    * @param k      number of clusters
    * @param random random number generator
    * @return the cluster centroids with weight based on the assigned points
    */
  def buildClusters(points: Seq[Point], k: Int)(implicit random: Random): Map[Point, (Int, Double)] =
    buildClusters(points, createRandomCentroids(points, k, random).zipWithIndex.map(t => (t._1, (t._2, 0.0))), k)

  /**
    * builds the clusters based on initial centroids
    *
    * @param points           the points to cluster
    * @param initialCentroids the initial centroids for clustering
    * @return the cluster centroids with weight based on the assigned points
    */
  def buildClusters(points: Seq[Point], initialCentroids: Seq[(Point, (Int, Double))], k: Int)(implicit random: Random): Map[Point, (Int, Double)] =
    updateClusters(
      points,
      initialCentroids.toMap, // NOTE duplicates eliminated here -> k can shrink -> handled in updateClusters
      k
    )

  /**
    * creates a random point of a given number of dimensions
    *
    * @param dim    number of dimensions
    * @param random random number generator
    * @return random point
    */
  def randomPoint(dim: Int, random: Random): Point = Point(Vector.fill(dim)(random.nextGaussian()))

  @tailrec
  private def ensureK(clusters: Map[Point, (Int, Double)], k: Int, iteration: Int)(implicit random: Random): Map[Point, (Int, Double)] =
    if (clusters.size == k) clusters // base case
    else if (clusters.size < k) {
      // recursively split the largest clusters until reaching k
      val largestCluster = clusters.maxBy { case (_, (_, weight)) => weight }
      val maxIndex = clusters.map { case (_, (index, _)) => index }.max
      val newClusterWeights = (clusters - largestCluster._1) ++ splitCluster(largestCluster, offsetFactor = iteration * 3, nextIndex = maxIndex + 1)
      ensureK(newClusterWeights, k, iteration + 1) // recurse to do further splits if needed
    }
    else // clusters.size > k (k has been decreased)
      clusters
        .toSeq
        .sortBy { case (_, (_, weight)) => weight * -1 } // sort by decreasing weight
        .take(k) // keep k largest clusters
        .toMap

  @tailrec
  private def updateClusters(points: Seq[Point], centroids: Map[Point, (Int, Double)], k: Int)(implicit random: Random): Map[Point, (Int, Double)] = {
    val centroidWeightsK = ensureK(centroids, k, iteration = 1) // add/remove centroids if clusterWeights.size differs from k

    // assign points to existing clusters
    val clusters: Map[Point, (Int, Seq[Point])] = assignPoints(points, centroidWeightsK.map(t => (t._1, t._2._1)))

    if (clusters.size < k) {
      // less points than clusters - add omitted centroids with empty point lists and return
      val result = clusters.map { case (centroid, (index, members)) => (centroid, (index, members.size.toDouble)) } ++
        centroidWeightsK
          .filter(t => !clusters.contains(t._1))
          .map { case (centroid, (index, _)) => (centroid, (index, 0.0)) }

      assert(result.size == k)
      result
    }
    else {
      val newClusters = clusters.map { case (c, (index, ps)) => (if (ps.isEmpty) c else updateCentroid(ps), (index, ps)) }
      val newCentroidsWithWeights = newClusters.map { case (c, (index, ps)) => (c, (index, ps.size.toDouble)) }

      if (clusters != newClusters)
        updateClusters(points, newCentroidsWithWeights, k) // iterate until centroids don't change
      else
        newCentroidsWithWeights
    }
  }

  private def assignPoints(points: Seq[Point], centroids: Iterable[(Point, Int)]): Map[Point, (Int, Seq[Point])] = {
    if (points.isEmpty) centroids.map { case (p, index) => (p, (index, Nil)) }.toMap
    else points
      .map { point => (point, getNearestCentroid(point, centroids)) }
      .groupBy { case (_, nearest) => nearest }
      .map { case ((centroid, index), members) => (centroid, (index, members.map { case (p, _) => p })) }
  }

  private def splitCluster(largestCluster: (Point, (Int, Double)), offsetFactor: Int, nextIndex: Int): Iterable[(Point, (Int, Double))] = {
    val centroid = largestCluster._1
    val weight = largestCluster._2._2
    val largestIndex = largestCluster._2._1
    val dims = centroid.features.indices.toVector
    val fs: Vector[Double] = centroid.features

    List(
      (Point(dims.map(d => fs(d) - valueOffset(fs(d), offsetFactor))), (largestIndex, weight / 2)),
      (Point(dims.map(d => fs(d) + valueOffset(fs(d), offsetFactor))), (nextIndex, weight / 2)),
    )
  }

  private def valueOffset(value: Double, factor: Int): Double = factor * 1e-14 * math.max(value, 1.0)

  private def getNearestCentroid(point: Point, centroids: Iterable[(Point, Int)]): (Point, Int) = {
    val byDistanceToPoint = new Ordering[(Point, Int)] {
      override def compare(p1: (Point, Int), p2: (Point, Int)): Int = p1._1.squaredDistanceTo(point) compareTo p2._1.squaredDistanceTo(point)
    }

    centroids min byDistanceToPoint
  }

  private def updateCentroid(members: Seq[Point]): Point = {
    assert(members.nonEmpty)

    val dim = members.head.features.size // NOTE: assumes all points have equal dimensions
    val zero = Point(Vector.fill(dim)(0.0))

    val (sum: Point, count: Int) = members.foldLeft((zero, 0)) {
      case ((cur_sum, cur_count), point) => (cur_sum + point, cur_count + 1)
    }

    sum / count
  }


}





