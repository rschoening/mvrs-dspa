package org.mvrs.dspa.jobs.clustering

import java.lang.Math.{pow, sqrt}

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
  def buildClusters(points: Seq[Point], k: Int)(implicit random: Random): Map[Point, Double] =
    buildClusters(points, createRandomCentroids(points, k, random).map((_, 0.0)), k)

  /**
    * builds the clusters based on initial centroids
    *
    * @param points           the points to cluster
    * @param initialCentroids the initial centroids for clustering
    * @return the cluster centroids with weight based on the assigned points
    */
  def buildClusters(points: Seq[Point], initialCentroids: Seq[(Point, Double)], k: Int)(implicit random: Random): Map[Point, Double] =
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
  private def ensureK(clusterWeights: Map[Point, Double], k: Int, iteration: Int)(implicit random: Random): Map[Point, Double] =
    if (clusterWeights.size == k) clusterWeights // base case
    else if (clusterWeights.size < k) {
      // recursively split the largest clusters until reaching k
      val largestCluster: (Point, Double) = clusterWeights.maxBy(_._2)
      val newClusterWeights = (clusterWeights - largestCluster._1) ++ splitCluster(largestCluster, offsetFactor = iteration * 3)
      ensureK(newClusterWeights, k, iteration + 1) // recurse to do further splits if needed
    }
    else // clusters > k (k has been decreased)
      clusterWeights
        .toSeq
        .sortBy(_._2 * -1) // sort by decreasing weight
        .take(k) // keep k largest clusters
        .toMap

  @tailrec
  private def updateClusters(points: Seq[Point], centroidWeights: Map[Point, Double], k: Int)(implicit random: Random): Map[Point, Double] = {
    val centroidWeightsK = ensureK(centroidWeights, k, iteration = 1) // add/remove centroids if clusterWeights.size differs from k

    // assign points to existing clusters
    val clusters = assignPoints(points, centroidWeightsK.keys)

    if (clusters.size < k) {
      // less points than clusters - add omitted centroids with empty point lists and return
      val result = clusters.map(t => (t._1, t._2.size.toDouble)) ++
        centroidWeightsK
          .filter(t => !clusters.contains(t._1))
          .map(t => (t._1, 0.0))

      assert(result.size == k)
      result
    }
    else {
      val newClusters = clusters.map { case (c, ps) => (if (ps.isEmpty) c else updateCentroid(ps), ps) }
      val newCentroidsWithWeights = newClusters.map { case (c, ps) => (c, ps.size.toDouble) }

      if (clusters != newClusters)
        updateClusters(points, newCentroidsWithWeights, k) // iterate until centroids don't change
      else
        newCentroidsWithWeights
    }
  }

  private def assignPoints(points: Seq[Point], centroids: Iterable[Point]): Map[Point, Seq[Point]] = {
    if (points.isEmpty) centroids.map((_, Nil)).toMap
    else points
      .map { point => (point, getNearestCentroid(point, centroids)) }
      .groupBy { case (_, centroid) => centroid }
      .map { case (centroid, members) => (centroid, members.map { case (p, _) => p }) }
  }

  private def splitCluster(largestCluster: (Point, Double), offsetFactor: Int): Iterable[(Point, Double)] = {
    val centroid = largestCluster._1
    val weight = largestCluster._2
    val dims = centroid.features.indices.toVector
    val fs: Vector[Double] = centroid.features

    List(
      (Point(dims.map(d => fs(d) - valueOffset(fs(d), offsetFactor))), weight / 2),
      (Point(dims.map(d => fs(d) + valueOffset(fs(d), offsetFactor))), weight / 2),
    )
  }

  private def valueOffset(value: Double, factor: Int): Double = factor * 1e-14 * math.max(value, 1.0)

  private def getNearestCentroid(point: Point, centroids: Iterable[Point]): Point = {
    val byDistanceToPoint = new Ordering[Point] {
      override def compare(p1: Point, p2: Point): Int = p1.squaredDistanceTo(point) compareTo p2.squaredDistanceTo(point)
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

  final case class Point(features: Vector[Double]) {

    def distanceTo(that: Point): Double = sqrt(squaredDistanceTo(that))

    def squaredDistanceTo(that: Point): Double =
      features
        .view
        .zip(that.features)
        .map { case (x0, x1) => pow(x0 - x1, 2) }
        .sum

    def +(that: Point) = Point(
      features
        .zip(that.features)
        .map { case (x0, x1) => x0 + x1 })

    def /(number: Int) = Point(features.map(_ / number))

    override def toString = s"Point(${features.mkString(", ")})"
  }

  object Point {
    def apply(values: Double*): Point = Point(Vector(values: _*)): Point
  }

}

