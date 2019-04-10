package org.mvrs.dspa.clustering

import java.lang.Math.{pow, sqrt}

import scala.annotation.tailrec
import scala.util.Random

/**
  * Adapted from [[https://gist.github.com/metanet/a385d42fd2cab9f3d20e]]
  *
  */
object KMeansClustering {

  def createRandomCentroids(points: Seq[Point], k: Int, random: Random = new Random()): Seq[Point] = {
    require(points.nonEmpty, "empty input")

    val randomIndices = collection.mutable.HashSet[Int]()

    if (points.nonEmpty) while (randomIndices.size < k) {
      randomIndices += random.nextInt(points.size)
    }

    points
      .zipWithIndex
      .filter { case (_, index) => randomIndices.contains(index) }
      .map { case (point, _) => point }
  }

  def buildClusters(points: Seq[Point], k: Int, random: Random = new Random()): Map[Point, Seq[Point]] =
    buildClusters(points, createRandomCentroids(points, k, random))

  def buildClusters(points: Seq[Point], initialCentroids: Seq[Point]): Map[Point, Seq[Point]] =
    updateClusters(points, initialCentroids.map { point => (point, Nil) }.toMap)

  @tailrec
  private def updateClusters(points: Seq[Point], prevClusters: Map[Point, Seq[Point]]): Map[Point, Seq[Point]] = {

    // assign points to existing clusters
    val nextClusters: Map[Point, Seq[Point]] =
      if (points.isEmpty) prevClusters
      else points
        .map { point => (point, getNearestCentroid(point, prevClusters.keys)) }
        .groupBy { case (_, centroid) => centroid }
        .map { case (centroid, members) => (centroid, members.map { case (p, _) => p }) }

    if (nextClusters.size < prevClusters.size) {
      // less points than clusters - add omitted centroids with empty point lists and return
      nextClusters ++ prevClusters.filter(t => !nextClusters.contains(t._1)).map(t => (t._1, Nil))
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

  case class Point(features: Vector[Double]) {

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

