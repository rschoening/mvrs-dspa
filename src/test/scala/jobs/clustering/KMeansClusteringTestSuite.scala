package jobs.clustering

import org.mvrs.dspa.jobs.clustering.KMeansClustering
import org.mvrs.dspa.jobs.clustering.KMeansClustering.Point
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class KMeansClusteringTestSuite extends FlatSpec with Matchers {
  // TODO add test for reducing k (keep largest clusters)
  // TODO add test for increasing k (split largest clusters, recursively)

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
    assert(clusters(Point(6.0, 4.0, 1.0)) == cluster1.size)
    assert(clusters(Point(0.0, 1.0, 1.0)) == cluster2.size)
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

    val clusters = KMeansClustering.buildClusters(List[Point](), centroids.map((_, 0.0)), centroids.size)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(centroids.toSet)(clusters.keys)
    assert(clusters.forall(_._2 == 0.0))
  }

  it should "handle points < k - always return k clusters" in {
    val points = List(Point(5, 5))

    val centroids = List(
      (Point(1, 1), 1.0),
      (Point(2, 2), 2.0),
      (Point(3, 3), 1.5)
    )

    val clusters = KMeansClustering.buildClusters(points, centroids, centroids.size)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(centroids.map(_._1).toSet)(clusters.keys)
    assert(clusters(Point(1, 1)) == 0.0)
    assertResult(1.0)(clusters(Point(3, 3)))
  }

  it should "handle centroids > k (reducing k) - return k largest clusters" in {
    val points = List(Point(5, 5))

    val centroids = List(
      (Point(1, 1), 1.0),
      (Point(2, 2), 2.0),
      (Point(3, 3), 3.0),
      (Point(4, 4), 0.0)
    )

    val clusters: Map[Point, Double] = KMeansClustering.buildClusters(points, centroids, 2)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(Set(Point(3.0, 3.0), Point(2.0, 2.0)))(clusters.keys)
    assertResult(1.0)(clusters(Point(3, 3)))
    assertResult(0.0)(clusters(Point(2, 2)))
  }

  it should "handle duplicate centroids" in {
    val points = List(Point(5, 5))

    val centroids = List(
      Point(1, 1),
      Point(2, 2),
      Point(2, 2)
    )

    val clusters = KMeansClustering.buildClusters(points, centroids.map((_, 0.0)), centroids.size)(new Random(137))

    println(clusters.mkString("\n"))

    assertResult(Set(Point(2.0, 2.0), Point(0.99999999999999, 0.99999999999999), Point(1.00000000000001, 1.00000000000001)))(clusters.keys)
    // assert(clusters(Point(1, 1)).isEmpty)
    assertResult(1.0)(clusters(Point(2, 2)))
    assert((clusters - Point(2, 2)).forall(_._2 == 0.0))
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
