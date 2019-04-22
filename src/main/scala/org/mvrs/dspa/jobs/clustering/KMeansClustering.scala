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
    * @return the cluster centroids with assigned points
    */
  def buildClusters(points: Seq[Point], k: Int)(implicit random: Random): Map[Point, Double] =
    buildClusters(points, createRandomCentroids(points, k, random).map((_, 0.0)), k)

  /**
    * builds the clusters based on initial centroids
    *
    * @param points           the points to cluster
    * @param initialCentroids the initial centroids for clustering
    * @return the cluster centroids with assigned points
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
  private def ensureK(points: Seq[Point], prevClusters: Map[Point, Double], k: Int, iteration: Int)(implicit random: Random): Map[Point, Double] =
    if (prevClusters.size == k) prevClusters
    else if (prevClusters.size < k) {

      // split the largest clusters recursively up to k
      val largestCluster: (Point, Double) = prevClusters.maxBy(_._2)
      val newClusters = (prevClusters - largestCluster._1) ++ splitCluster(largestCluster, offsetFactor = iteration)
      ensureK(points, newClusters, k, iteration + 1)
    }
    else prevClusters.toSeq.sortBy(t => t._2 * -1).take(k).toMap // keep k largest clusters

  @tailrec
  private def updateClusters(points: Seq[Point], clusterWeights: Map[Point, Double], k: Int)(implicit random: Random): Map[Point, Double] = {
    val clusterWeightsK = ensureK(points, clusterWeights, k, iteration = 1) // add/remove centroids if clusterWeights.size differs from k
    assert(k == clusterWeightsK.size)

    // assign points to existing clusters
    val pointAssignment = assignPoints(points, clusterWeightsK.keys)

    if (pointAssignment.size < k) {
      // TODO REVISE just recurse?

      // less points than clusters - add omitted centroids with empty point lists and return
      val result = pointAssignment.map(t => (t._1, t._2.size.toDouble)) ++
        clusterWeightsK
          .filter(t => !pointAssignment.contains(t._1))
          .map(t => (t._1, 0.0))

      assert(result.size == k)
      result
    }
    else {
      val updatedCentroids = pointAssignment.map {
        case (c, members) => if (members.isEmpty) (c, Nil) else (updateCentroid(members), members)
      }
      val updatedWeights = updatedCentroids.map { case (c, members) => (c, members.size.toDouble) }

      if (pointAssignment != updatedCentroids) updateClusters(points, updatedWeights, k) // iterate until centroids don't change
      else updatedWeights
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
    // NOTE clusters may have no points assigned at this time - have to work with centroid alone
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

