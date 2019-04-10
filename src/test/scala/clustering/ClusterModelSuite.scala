package clustering

import org.mvrs.dspa.clustering.KMeansClustering.Point
import org.mvrs.dspa.clustering.{Cluster, ClusterModel}
import org.scalatest.{FlatSpec, Matchers}

class ClusterModelSuite extends FlatSpec with Matchers {
  "a cluster model" should "do a simple update" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 1)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 1.0)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(1.5, 1.5))(newModel.clusters.head.centroid)
  }

  "a cluster model" should "do an update with decay" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 2)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 0.5)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(1.5, 1.5))(newModel.clusters.head.centroid)
  }

  "a cluster model" should "do a weighted update" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 9)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 1.0)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(1.1, 1.1))(newModel.clusters.head.centroid)
  }
}
