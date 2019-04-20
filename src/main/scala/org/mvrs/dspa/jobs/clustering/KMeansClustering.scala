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
    require(points.nonEmpty, "empty input")

    val uniquePoints = points.toSet
    val resultSet = mutable.Set[Point]()

    if (uniquePoints.size < k) {
      val dim = points.head.features.size
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

  /**
    * builds the clusters based on randomly generated centroids
    *
    * @param points the points to cluster
    * @param k      number of clusters
    * @param random random number generator
    * @return the cluster centroids with assigned points
    */
  def buildClusters(points: Seq[Point], k: Int, random: Random = new Random()): Map[Point, Seq[Point]] =
    buildClusters(points, createRandomCentroids(points, k, random))

  /**
    * builds the clusters based on initial centroids
    *
    * @param points           the points to cluster
    * @param initialCentroids the initial centroids for clustering
    * @return the cluster centroids with assigned points
    */
  def buildClusters(points: Seq[Point], initialCentroids: Seq[Point]): Map[Point, Seq[Point]] =
    updateClusters(points, initialCentroids.map((_, Nil)).toMap)

  /**
    * creates a random point of a given number of dimensions
    *
    * @param dim    number of dimensions
    * @param random random number generator
    * @return random point
    */
  def randomPoint(dim: Int, random: Random): Point = Point(Vector.fill(dim)(random.nextGaussian()))

  @tailrec
  private def updateClusters(points: Seq[Point], prevClusters: Map[Point, Seq[Point]]): Map[Point, Seq[Point]] = {
    val k = prevClusters.size

    // assign points to existing clusters
    val nextClusters =
      if (points.isEmpty) prevClusters
      else points
        .map { point => (point, getNearestCentroid(point, prevClusters.keys)) }
        .groupBy { case (_, centroid) => centroid }
        .map { case (centroid, members) => (centroid, members.map { case (p, _) => p }) }

    if (nextClusters.size < k) {
      // less points than clusters - add omitted centroids with empty point lists and return
      val result = nextClusters ++
        prevClusters
          .filter(t => !nextClusters.contains(t._1))
          .map(t => (t._1, Nil))

      assert(result.size == k)
      result
    }
    else if (prevClusters != nextClusters) {
      // point assignment has changed - update cluster centroids
      val nextClustersWithBetterCentroids = nextClusters.map { case (_, members) => updateCentroid(members) }

      updateClusters(points, nextClustersWithBetterCentroids) // iterate until centroids don't change
    } else prevClusters
  }

  private def getNearestCentroid(point: Point, centroids: Iterable[Point]): Point = {
    val byDistanceToPoint = new Ordering[Point] {
      override def compare(p1: Point, p2: Point): Int = p1.squaredDistanceTo(point) compareTo p2.squaredDistanceTo(point)
    }

    centroids min byDistanceToPoint
  }

  private def updateCentroid(members: Seq[Point]): (Point, Seq[Point]) = {
    assert(members.nonEmpty)

    val dim = members.head.features.size // NOTE: assumes all points have equal dimensions
    val zero = Point(Vector.fill(dim)(0.0))

    val (sum: Point, count: Int) = members.foldLeft((zero, 0)) {
      case ((cur_sum, cur_count), point) => (cur_sum + point, cur_count + 1)
    }

    (sum / count, members)
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

