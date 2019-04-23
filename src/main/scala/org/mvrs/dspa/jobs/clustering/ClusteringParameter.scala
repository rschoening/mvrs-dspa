package org.mvrs.dspa.jobs.clustering

import scala.util.Try
import scala.util.matching.Regex

sealed trait ClusteringParameter {
  val key: String
}

final case class ClusteringParameterK(k: Int) extends ClusteringParameter {
  val key = "k"
}

final case class ClusteringParameterDecay(decay: Double) extends ClusteringParameter {
  val key = "decay"
}

final case class ClusteringParameterLabel(clusterIndex: Int, label: String) extends ClusteringParameter {
  val key = s"label$clusterIndex"
}

object ClusteringParameter {
  private val labelPattern: Regex = "(\\s*label\\s*:\\s*)(\\d*)".r

  def parse(line: String): List[Either[Throwable, ClusteringParameter]] =
    line.split('=').map(_.trim.toLowerCase).toList match {
      case "k" :: v :: Nil => List(Try(ClusteringParameterK(v.toInt)).toEither)
      case "decay" :: v :: Nil => List(Try(ClusteringParameterDecay(v.toDouble)).toEither)
      case labelPattern(_, index) :: v :: Nil => List(Try(ClusteringParameterLabel(index.toInt, v)).toEither)
      case "" :: Nil => Nil // ignore empty line
      case _ => List(Left(new Exception(s"Invalid parameter line: $line")))
    }
}
