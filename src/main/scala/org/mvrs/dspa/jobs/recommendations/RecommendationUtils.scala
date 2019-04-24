package org.mvrs.dspa.jobs.recommendations

import java.util.Base64

import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.mvrs.dspa.Settings

object RecommendationUtils {
  val tagPrefix = "T"

  lazy val minHasher: MinHasher32 = createMinHasher(
    Settings.config.getInt("jobs.recommendation.minhash-num-hashes"),
    Settings.config.getDouble("jobs.recommendation.lsh-target-threshold")
  )

  def getTopN(minHashSignature: MinHashSignature,
              candidates: Seq[(Long, MinHashSignature)],
              minHasher: MinHasher32,
              n: Int,
              minimumSimilarity: Double): Seq[(Long, Double)] = {
    candidates
      .map { case (id, signature) => (id, minHasher.similarity(minHashSignature, signature)) }
      .filter(t => t._2 >= minimumSimilarity)
      .sortBy(-1 * _._2)
      .take(n)
  }

  def getMinHashSignature(features: Iterable[String], minHasher: MinHasher32): MinHashSignature =
    minHasher.combineAll(features.map(minHasher.init))

  def decodeMinHashSignature(base64: String) = MinHashSignature(Base64.getDecoder.decode(base64))

  def createMinHasher(numHashes: Int, targetThreshold: Double): MinHasher32 = {
    require(numHashes > 0)
    require(targetThreshold > 0)

    new MinHasher32(numHashes, MinHasher.pickBands(targetThreshold, numHashes))
  }

  def toFeature(input: Long, prefix: String): String = s"$prefix$input"
}
