package org.mvrs.dspa.jobs.clustering

import scala.util.Try
import scala.util.matching.Regex

/**
  * Base class for clustering parameters.
  *
  * @param key The parameter key
  * @note these classes are serialized using generic serialization
  */
sealed abstract class ClusteringParameter(val key: String)

/**
  * Parameter value for k, the number of clusters to produce
  *
  * @param k configured value of k
  */
final case class ClusteringParameterK(k: Int) extends ClusteringParameter("k")

/**
  * Parameter value for the decay factor to reduce the weight of the previous cluster in the calculation of the new cluster's weight
  *
  * @param decay Decay factor
  */
final case class ClusteringParameterDecay(decay: Double) extends ClusteringParameter("decay")

/**
  * Parameter value for the label of a cluster, determined based on interpretation of the cluster centroids and
  * assigned persons. Special labels could be used for triggering alerts etc.
  *
  * @param clusterIndex The cluster index to label
  * @param label        The label
  */
final case class ClusteringParameterLabel(clusterIndex: Int,
                                          label: String) extends ClusteringParameter(s"label$clusterIndex")

/**
  * Companion object
  */
object ClusteringParameter {
  /**
    * Regex pattern for label lines (e.g. "label:3 = werewolves" to label the third cluster as representing werewolves)
    */
  private val labelPattern: Regex = "(\\s*label\\s*:\\s*)(\\d*)".r

  /**
    * Parse a clustering parameter file line
    *
    * @param line The input string
    * @return A list of either a parse errors, or successfully parsed clustering parameters
    */
  def parse(line: String): List[Either[Throwable, ClusteringParameter]] =
    if (line.trim.startsWith("#"))
      Nil // comment line, ignore
    else line.split('=').map(_.trim.toLowerCase).toList match {
      case "k" :: v :: Nil => List(Try(ClusteringParameterK(v.toInt)).toEither)
      case "decay" :: v :: Nil => List(Try(ClusteringParameterDecay(v.toDouble)).toEither)
      case labelPattern(_, index) :: v :: Nil => List(Try(ClusteringParameterLabel(index.toInt, v)).toEither)
      case "" :: Nil => Nil // ignore empty line
      case _ => List(Left(new Exception(s"Invalid parameter line: $line")))
    }
}
