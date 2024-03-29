package jobs.clustering

import org.mvrs.dspa.jobs.clustering.KMeansClustering
import org.mvrs.dspa.model.Point
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class KMeansClusteringTestSuite extends FlatSpec with Matchers {
  "kmeans" should "create expected clusters" in {

    val cluster1 =
      List(
        Point(6, 2, 0),
        Point(4, 4, 3),
        Point(8, 6, 0)
      )
    val cluster2 =
      List(
        Point(0, 0, 3),
        Point(0, 0, 0),
        Point(0, 3, 0)
      )

    val points = cluster1 ++ cluster2

    val k = 2
    val clusters = KMeansClustering.buildClusters(points, k)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assert(clusters(Point(6.0, 4.0, 1.0))._2 == cluster1.size)
    assert(clusters(Point(0.0, 1.0, 1.0))._2 == cluster2.size)
  }

  it should "produce an exception when generating random clusters based on empty input" in {
    assertThrows[IllegalArgumentException] {
      KMeansClustering.buildClusters(List[Point](), k = 2)(new Random(137))
    }
  }

  it should "handle empty input with predefined clusters" in {
    val centroids = List(
      Point(1, 1),
      Point(2, 2)
    )

    val k = centroids.size
    val clusters = KMeansClustering.buildClusters(List[Point](), centroids.map((_, (-1, 0.0))), k)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assertResult(centroids.toSet)(clusters.keys)
    assert(clusters.forall(_._2._2 == 0.0))
  }

  it should "handle points < k - always return k clusters" in {
    val points = List(Point(5, 5))

    val centroids = List(
      (Point(1, 1), (0, 1.0)),
      (Point(2, 2), (1, 2.0)),
      (Point(3, 3), (2, 1.5))
    )

    val k = centroids.size
    val clusters = KMeansClustering.buildClusters(points, centroids, k)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assertResult(centroids.map(_._1).toSet)(clusters.keys)
    assert(clusters(Point(1, 1))._2 == 0.0)
    assertResult((2, 1.0))(clusters(Point(3, 3)))
  }

  it should "handle centroids > k (reducing k) - return k largest clusters" in {
    val points = List(Point(5, 5))

    val centroids = List(
      (Point(1, 1), (0, 1.0)),
      (Point(2, 2), (1, 2.0)),
      (Point(3, 3), (2, 3.0)),
      (Point(4, 4), (3, 0.0))
    )

    val k = 2
    val clusters: Map[Point, (Int, Double)] = KMeansClustering.buildClusters(points, centroids, k)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assertResult(Set(Point(3.0, 3.0), Point(2.0, 2.0)))(clusters.keys)
    assertResult((2, 1.0))(clusters(Point(3, 3)))
    assertResult((1, 0.0))(clusters(Point(2, 2)))
  }

  it should "handle centroids < k (increasing k) - split largest clusters" in {
    val points = List(
      Point(0, 0),
      Point(1, 1),
      Point(2, 2),
      Point(3, 3),
      Point(4, 4),
      Point(5, 5)
    )

    val centroids = List(
      (Point(1, 1), (0, 1.0)),
      (Point(2, 2), (1, 2.0)),
      (Point(3, 3), (2, 3.0))
    )

    val k = 5
    val clusters: Map[Point, (Int, Double)] = KMeansClustering.buildClusters(points, centroids, 5)(new Random(137))

    // should first split (3,3) then (2,2)

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assertResult(Set(
      Point(1.0, 1.0),
      Point(3.00000000000009, 3.00000000000009),
      Point(1.99999999999988, 1.99999999999988),
      Point(2.99999999999991, 2.99999999999991),
      Point(2.00000000000012, 2.00000000000012)
    ))(clusters.keySet)
  }

  it should "handle duplicate centroids" in {
    val points = List(Point(5, 5))

    val centroids = List(
      (Point(1, 1), (0, 0.0)),
      (Point(2, 2), (1, 0.0)),
      (Point(2, 2), (2, 0.0))
    )

    val k = centroids.size
    val clusters = KMeansClustering.buildClusters(points, centroids, k)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assertResult(Set(Point(2.0, 2.0), Point(0.99999999999997, 0.99999999999997), Point(1.00000000000003, 1.00000000000003)))(clusters.keys)
    assertResult((2, 1.0))(clusters(Point(2, 2)))
    assert((clusters - Point(2, 2)).forall(_._2._2 == 0.0))
  }

  "random centroid generation" should "create unique points also with non-unique input points" in {
    val k = 4
    val points = List.fill[Point](k)(Point(1, 1))

    val centroids = KMeansClustering.createRandomCentroids(points, k, new Random(137)).toSet

    println(centroids.mkString("\n"))

    assertResult(k)(centroids.size)
    assert(points.forall(centroids.contains))
  }

  it should "create unique points also with points < k" in {
    val k = 4
    val points = List(Point(1, 1), Point(2, 2))

    val centroids = KMeansClustering.createRandomCentroids(points, k, new Random(137)).toSet

    println(centroids.mkString("\n"))

    assertResult(k)(centroids.size)
    assert(points.forall(centroids.contains))

  }
}
