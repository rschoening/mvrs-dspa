package org.mvrs.dspa.jobs.clustering

import scala.util.{Failure, Try}

sealed trait ClusteringParameter

final case class ClusteringParameterK(k: Int) extends ClusteringParameter

final case class ClusteringParameterDecay(decay: Double) extends ClusteringParameter

final case class ClusteringParameterLabel(clusterIndex: Int, label: String) extends ClusteringParameter

object ClusteringParameter {
  def parse(line: String): List[Either[Throwable, ClusteringParameter]] =
    line.split('=').map(_.trim.toLowerCase).toList match {
      case "k" :: v :: Nil => List(Try(ClusteringParameterK(v.toInt)).toEither)
      case "decay" :: v :: Nil => List(Try(ClusteringParameterDecay(v.toDouble)).toEither)
      case "label" :: v :: Nil => List(tryParseLabel(v).toEither)
      case "" :: Nil => Nil // ignore empty line
      case _ => List(Left(new Exception(s"Invalid parameter line: $line")))
    }

  private def tryParseLabel(value: String): Try[ClusteringParameterLabel] =
    value.split(':').map(_.trim).toList match {
      case clusterIndex :: label :: Nil => Try(ClusteringParameterLabel(clusterIndex.toInt, label))
      case _ => Failure(new Exception(s"Invalid label: $value"))
    }
}
