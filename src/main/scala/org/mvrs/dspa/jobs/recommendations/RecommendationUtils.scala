package org.mvrs.dspa.jobs.recommendations

import java.util.Base64

import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}

object RecommendationUtils {
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

  def createMinHasher(numHashes: Int = 100, targetThreshold: Double = 0.2): MinHasher32 =
    new MinHasher32(numHashes, MinHasher.pickBands(targetThreshold, numHashes))

  def toFeature(input: Long, prefix: String): String = s"$prefix$input"
}
