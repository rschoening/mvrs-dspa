package org.mvrs.dspa.jobs.recommendations

import java.util.Base64

import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import javax.annotation.Nonnegative
import org.mvrs.dspa.Settings

object RecommendationUtils {
  val tagPrefix = "T"

  /**
    * The MinHasher to use for both the static data preparation batch job and the recommendations streaming job.
    */
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

  /**
    * Calculates the MinHash signature for a set of features, using a given MinHasher
    *
    * @param features  The set of features
    * @param minHasher The MinHasher
    * @return The MinHash signature
    */
  def getMinHashSignature(features: Iterable[String], minHasher: MinHasher32): MinHashSignature =
    minHasher.combineAll(features.map(minHasher.init))

  /**
    * Decodes a MinHash signature from its Base64 representation
    *
    * @param base64 Base64-encoded MinHash signature (used for storing in ElasticSearch)
    * @return Decoded MinHash signature (wrapping a byte array)
    */
  def decodeMinHashSignature(base64: String): MinHashSignature = MinHashSignature(Base64.getDecoder.decode(base64))

  /**
    * Creates a MinHasher for calculating MinHash signatures of sets (see
    * [[https://twitter.github.io/algebird/datatypes/approx/min_hasher.html]])
    *
    * ===From twitter's algebird library:===
    *
    * Instances of MinHasher can create, combine, and compare fixed-sized signatures of arbitrarily sized sets.
    *
    * A signature is represented by a byte array of approx maxBytes size.
    * You can initialize a signature with a single element, usually a Long or String.
    * You can combine any two set's signatures to produce the signature of their union.
    * You can compare any two set's signatures to estimate their Jaccard similarity.
    * You can use a set's signature to estimate the number of distinct values in the set.
    * You can also use a combination of the above to estimate the size of the intersection of
    * two sets from their signatures.
    * The more bytes in the signature, the more accurate all of the above will be.
    *
    * You can also use these signatures to quickly find similar sets without doing
    * n^2^ comparisons. Each signature is assigned to several buckets; sets whose signatures
    * end up in the same bucket are likely to be similar. The targetThreshold controls
    * the desired level of similarity - the higher the threshold, the more efficiently
    * you can find all the similar sets.
    *
    * This abstract superclass is generic with regards to the size of the hash used.
    * Depending on the number of unique values in the domain of the sets, you may want
    * a MinHasher16, a MinHasher32, or a new custom subclass.
    *
    * This implementation is modeled after Chapter 3 of Ullman and Rajaraman's Mining of Massive Datasets:
    * http://infolab.stanford.edu/~ullman/mmds/ch3a.pdf
    *
    * @param numHashes       The number of hashes in the signature
    * @param targetThreshold The target threshold controlling the desired level of similarity
    * @return MinHasher instance
    */
  def createMinHasher(@Nonnegative numHashes: Int, @Nonnegative targetThreshold: Double): MinHasher32 = {
    require(numHashes > 0)
    require(targetThreshold > 0)

    new MinHasher32(numHashes, MinHasher.pickBands(targetThreshold, numHashes))
  }

  /**
    * Creates a feature string based on a feature id and type prefix
    *
    * @param input  The feature id (e.g. interest or place id)
    * @param prefix The prefix for this type of feature (e.g. "T" for tags)
    * @return
    */
  def toFeature(input: Long, prefix: String): String = s"$prefix$input"
}
