package clustering

import org.mvrs.dspa.clustering.KMeansClustering
import org.mvrs.dspa.clustering.KMeansClustering.Point
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class KMeansClusteringSuite extends FlatSpec with Matchers {
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
    val clusters = KMeansClustering.buildClusters(points, k, new Random(137))

    println(clusters.mkString("\n"))

    assertResult(k)(clusters.size)
    assert(clusters(Point(6.0, 4.0, 1.0)).toSet == cluster1.toSet)
    assert(clusters(Point(0.0, 1.0, 1.0)).toSet == cluster2.toSet)
  }

  it should "produce an exception when generating random clusters based on empty input" in {
    assertThrows[IllegalArgumentException] {
      KMeansClustering.buildClusters(List[Point](), k = 2, new Random(137))
    }
  }

  it should "handle empty input with predefined clusters" in {
    val centroids = List(
      Point(1, 1),
      Point(2, 2)
    )

    val clusters = KMeansClustering.buildClusters(List[Point](), centroids)

    println(clusters.mkString("\n"))

    assertResult(centroids.toSet)(clusters.keys)
    assert(clusters.forall(_._2.isEmpty))
  }

  it should "handle points < k - always return k clusters" in {
    val points = List(Point(5, 5))

    val centroids = List(
      Point(1, 1),
      Point(2, 2)
    )

    val clusters = KMeansClustering.buildClusters(points, centroids)

    println(clusters.mkString("\n"))

    assertResult(centroids.toSet)(clusters.keys)
    assert(clusters(Point(1, 1)).isEmpty)
    assertResult(List(Point(5, 5)))(clusters(Point(2, 2)))
  }

}
