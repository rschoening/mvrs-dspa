package jobs.clustering

import org.mvrs.dspa.jobs.clustering.KMeansClustering.Point
import org.mvrs.dspa.jobs.clustering.{Cluster, ClusterModel}
import org.scalatest.{FlatSpec, Matchers}

class ClusterModelSuite extends FlatSpec with Matchers {
  "The cluster model" can "do a simple update (decay = 1, equal weights)" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 1)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 1.0)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(1.5, 1.5))(newModel.clusters.head.centroid)
    assertResult(2)(newModel.clusters.head.weight)
  }

  it can "do an update with decay" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 2)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 0.5)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(1.5, 1.5))(newModel.clusters.head.centroid)
    assertResult(2)(newModel.clusters.head.weight)
  }

  it can "do a weighted update (decay = 1)" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 9)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 1.0)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(1.1, 1.1))(newModel.clusters.head.centroid)
    assertResult(10)(newModel.clusters.head.weight)
  }

  it can "ignore previous model (decay = 0)" in {
    val model = ClusterModel(Vector(Cluster(0, Point(1.0, 1.0), weight = 9)))
    val newModel = model.update(Vector(Cluster(0, Point(2.0, 2.0), weight = 1)), decay = 0.0)

    assertResult(1)(newModel.clusters.size)
    assertResult(Point(2, 2))(newModel.clusters.head.centroid)
    assertResult(1)(newModel.clusters.head.weight)
  }

  it can "correctly classify points" in {
    val model = ClusterModel(
      Vector(
        Cluster(0, Point(1.0, 10.0, 100.0), weight = 1),
        Cluster(1, Point(2.0, 11.0, 101.0), weight = 1),
        Cluster(2, Point(100.0, 200.0, 300.0), weight = 1),
      )
    )

    assertResult(0)(model.classify(Point(1.4, 10.4, 100.4)).index)
    assertResult(1)(model.classify(Point(1.6, 10.6, 100.6)).index)
    assertResult(2)(model.classify(Point(99, 199, 299)).index)
  }
}
